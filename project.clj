(defproject org.onyxplatform/onyx-local-rt "0.9.15.5-SNAPSHOT"
  :description "A local, pure, deterministic runtime for Onyx"
  :url "https://github.com/onyx-platform/onyx-local-rt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.9.16-20161208.183703-3"]
                 [clojure-future-spec "1.9.0-alpha14"]
                 [com.stuartsierra/dependency "0.2.0"]]
  :plugins [[codox "0.8.8"]])
