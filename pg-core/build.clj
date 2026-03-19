(ns build
  (:require [clojure.tools.build.api :as b]))

(defn javac [_]
  (b/javac {:src-dirs ["src/java"]
            :class-dir "classes"
            :basis (b/create-basis {:project "deps.edn"})
            :javac-opts ["--release" "16"
                         "-Xlint:unchecked"]}))
