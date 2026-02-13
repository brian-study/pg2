(ns pg.async-messages-test
  "Tests for async message handling during readTypesProcess and pool
   connection validation. Covers fixes for connection corruption caused
   by NoticeResponse, ParameterStatus, and NotificationResponse arriving
   during COPY-based type resolution.

   Fix 1 (root cause): readTypesProcess handles async messages
   Fix 2 (preventive): VOID type pre-registered, avoids readTypesProcess
   Fix 3 (defensive): null-check in sendBind for clear error
   Fix 4 (safety net): hasUnreadData pool validation"
  (:import
   org.pg.Connection
   org.pg.error.PGError)
  (:require
   [clojure.test :refer [deftest is testing use-fixtures]]
   [pg.core :as pg]
   [pg.pool :as pool]
   [pg.integration :as itg]))


(use-fixtures :each itg/fix-multi-port)


;; ============================================================
;; Fix 2: VOID type processor (OID 2278)
;;
;; pg_notify() returns VOID. With a pre-registered VOID processor,
;; the return value is decoded as nil. Without it, pg2 returns raw
;; bytes and triggers readTypesProcess to resolve the OID.
;; ============================================================

(deftest test-void-type-pg-notify-returns-nil
  (testing "pg_notify with parameterized query decodes VOID as nil"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (let [res (pg/execute conn
                            "SELECT pg_notify($1, $2)"
                            {:params ["test_void_chan" "hello"]})]
        ;; With the Void processor, VOID is decoded as nil.
        ;; Without it, this would be a byte array.
        (is (= [{:pg_notify nil}] res))))))


(deftest test-void-type-no-readTypesProcess-needed
  (testing "VOID OID is known so readTypesProcess is never triggered"
    ;; If VOID is pre-registered, calling pg_notify with params
    ;; should NOT trigger the COPY-based type resolution at all.
    ;; We verify this indirectly: repeated calls on the same connection
    ;; all succeed, and LISTEN + pg_notify on same connection doesn't
    ;; cause issues (which it would if readTypesProcess ran while a
    ;; notification was pending).
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "LISTEN test_void_chan")
      (dotimes [i 5]
        (let [res (pg/execute conn
                              "SELECT pg_notify($1, $2)"
                              {:params ["test_void_chan" (str "msg-" i)]})]
          (is (= [{:pg_notify nil}] res)
              (str "iteration " i " should decode VOID as nil"))))

      ;; Connection is healthy and notifications were delivered
      (is (= [{:one 1}] (pg/execute conn "select 1 as one")))
      (let [notifications (pg/drain-notifications conn)]
        (is (= 5 (count notifications)))))))


;; ============================================================
;; Fix 1: Async messages during readTypesProcess
;;
;; The real corruption requires a NotificationResponse to arrive
;; during the COPY flow in readTypesProcess. This requires two
;; connections: one LISTENing that triggers readTypesProcess, and
;; another that sends NOTIFY concurrently.
;;
;; Since readTypesProcess only runs for unknown OIDs, and Fix 2
;; pre-registers VOID, we need a different trigger. We use a
;; custom type (domain or enum) that forces type resolution.
;; ============================================================

(deftest test-notification-during-type-resolution
  (testing "NotificationResponse during readTypesProcess doesn't corrupt connection"
    ;; Create a custom type to force readTypesProcess on first use
    (pg/with-connection [setup-conn itg/*CONFIG-BIN*]
      (pg/execute setup-conn "DROP TYPE IF EXISTS test_async_enum CASCADE")
      (pg/execute setup-conn "CREATE TYPE test_async_enum AS ENUM ('a', 'b', 'c')"))

    (let [channel "test_async_notify"
          errors  (atom [])
          listener-config (assoc itg/*CONFIG-BIN*
                                 :fn-notification (fn [_msg] nil))]
      (try
        ;; Spin up a background thread that keeps sending notifications
        ;; rapidly. This maximizes the chance of a NotificationResponse
        ;; arriving during readTypesProcess on the listener connection.
        (pg/with-connection [notifier itg/*CONFIG-TXT*]
          (let [stop?  (atom false)
                sender (future
                         (while (not @stop?)
                           (try
                             (pg/execute notifier
                                        (format "NOTIFY %s, 'ping'" channel))
                             (Thread/sleep 1)
                             (catch Exception _))))]
            (try
              ;; On separate connections: LISTEN, then trigger type
              ;; resolution with the custom enum. Each new connection
              ;; hasn't seen this OID, so readTypesProcess WILL run.
              ;; If it doesn't handle NotificationResponse, it throws
              ;; "Unexpected message in readTypes" and corrupts the stream.
              (dotimes [_ 20]
                (try
                  (pg/with-connection [conn listener-config]
                    (pg/execute conn (str "LISTEN " channel))
                    (let [res (pg/execute conn
                                         "SELECT 'a'::test_async_enum AS val")]
                      (is (some? res)))
                    (let [res (pg/execute conn "select 1 as one")]
                      (is (= [{:one 1}] res))))
                  (catch Exception e
                    (swap! errors conj e))))
              (finally
                (reset! stop? true)
                @sender))))

        (finally
          ;; Clean up
          (pg/with-connection [conn itg/*CONFIG-BIN*]
            (pg/execute conn "DROP TYPE IF EXISTS test_async_enum CASCADE"))))

      (is (empty? @errors)
          (str "Got " (count @errors) " errors: "
               (mapv ex-message @errors))))))


(deftest test-notice-during-type-resolution
  (testing "NoticeResponse during type resolution doesn't corrupt connection"
    ;; Create a function that raises NOTICE and uses a custom type
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "DROP TYPE IF EXISTS test_notice_enum CASCADE")
      (pg/execute conn "CREATE TYPE test_notice_enum AS ENUM ('x', 'y')")
      (pg/execute conn "
        CREATE OR REPLACE FUNCTION test_notice_and_enum()
        RETURNS test_notice_enum AS $$
        BEGIN
          RAISE NOTICE 'notice during type resolution';
          RETURN 'x'::test_notice_enum;
        END;
        $$ LANGUAGE plpgsql"))

    (let [notices (atom [])
          config+ (assoc itg/*CONFIG-BIN*
                         :fn-notice (fn [msg] (swap! notices conj msg)))]
      ;; Each new connection must resolve the enum OID via readTypesProcess.
      ;; The function also raises a NOTICE. If the NOTICE arrives during
      ;; the COPY flow, readTypesProcess must handle it.
      (dotimes [_ 5]
        (pg/with-connection [conn config+]
          (let [res (pg/execute conn "SELECT test_notice_and_enum() AS val")]
            (is (some? res)))
          ;; Connection still healthy
          (is (= [{:one 1}] (pg/execute conn "select 1 as one"))))))

    ;; Clean up
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "DROP FUNCTION IF EXISTS test_notice_and_enum()")
      (pg/execute conn "DROP TYPE IF EXISTS test_notice_enum CASCADE"))))


;; ============================================================
;; Fix 1 + Fix 2: Pool stress test
;;
;; Multiple threads doing pg_notify + queries through a pool.
;; This is the real-world pattern that triggered the bug in
;; production: concurrent LISTEN/NOTIFY with pooled connections.
;; ============================================================

(deftest test-pool-concurrent-notify-stress
  (testing "concurrent pg_notify and queries via pool don't corrupt connections"
    (pool/with-pool [pool (assoc itg/*CONFIG-BIN*
                                 :pool-min-size 2
                                 :pool-max-size 4)]
      (let [channel "stress_chan"
            errors  (atom [])]

        ;; Hold one connection as a listener
        (pool/with-connection [listener pool]
          (pg/execute listener (str "LISTEN " channel))

          ;; Concurrent workers: pg_notify + regular queries
          (let [futures
                (doall
                 (for [i (range 20)]
                   (future
                     (try
                       (pool/with-connection [conn pool]
                         (pg/execute conn
                                    "SELECT pg_notify($1, $2)"
                                    {:params [channel (str "msg-" i)]})
                         ;; Follow up with a parameterized query to verify
                         ;; the connection isn't corrupted
                         (pg/execute conn
                                    "select $1::int as val"
                                    {:params [i]}))
                       (catch Exception e
                         (swap! errors conj e))))))]

            (doseq [f futures] @f)

            (is (empty? @errors)
                (str "Got " (count @errors) " errors: "
                     (mapv ex-message @errors)))

            ;; Listener still works
            (is (= [{:one 1}]
                   (pg/execute listener "select 1 as one")))))))))


;; ============================================================
;; Fix 4: hasUnreadData pool validation
;; ============================================================

(deftest test-has-unread-data-clean-connection
  (testing "a clean connection reports no unread data"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "select 1 as one")
      (is (false? (.hasUnreadData ^Connection conn))))))


(deftest test-pool-connections-healthy-after-notify
  (testing "pool connections are healthy after pg_notify"
    (pool/with-pool [pool (assoc itg/*CONFIG-BIN*
                                 :pool-min-size 1
                                 :pool-max-size 1)]
      (let [id1 (atom nil)
            id2 (atom nil)]

        ;; Use connection for pg_notify
        (pool/with-connection [conn pool]
          (reset! id1 (pg/id conn))
          (pg/execute conn
                      "SELECT pg_notify($1, $2)"
                      {:params ["test_healthy" "hello"]})
          ;; No unread data after pg_notify
          (is (false? (.hasUnreadData ^Connection conn))))

        ;; Same connection reused (pool size 1)
        (pool/with-connection [conn pool]
          (reset! id2 (pg/id conn))
          (is (= [{:one 1}] (pg/execute conn "select 1 as one"))))

        (is (= @id1 @id2)
            "same connection should be reused")))))


;; ============================================================
;; Fix 3: Clear error on protocol desync
;;
;; sendBind throws PGError with diagnostic info instead of NPE
;; when parameterDescription is null. This is hard to trigger
;; without corrupting a connection, so we just verify the normal
;; prepared statement flow works and leaves no unread data.
;; ============================================================

(deftest test-prepared-statement-lifecycle
  (testing "prepared statements work correctly through full lifecycle"
    (pg/with-connection [conn itg/*CONFIG-BIN*]
      (pg/execute conn "create temp table test_ps (id int, name text)")
      (pg/execute conn
                  "insert into test_ps (id, name) values ($1, $2)"
                  {:params [1 "alice"]})
      (pg/execute conn
                  "insert into test_ps (id, name) values ($1, $2)"
                  {:params [2 "bob"]})
      (let [res (pg/execute conn
                            "select * from test_ps order by id"
                            {:params []})]
        (is (= [{:id 1, :name "alice"}
                {:id 2, :name "bob"}]
               res)))

      ;; Connection clean after all operations
      (is (false? (.hasUnreadData ^Connection conn))))))
