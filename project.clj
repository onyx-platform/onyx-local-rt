(defproject org.onyxplatform/onyx-local-rt "0.10.0.0"
  :description "A local, pure, deterministic runtime for Onyx"
  :url "https://github.com/onyx-platform/onyx-local-rt"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"snapshots" {:url "https://clojars.org/repo"
                              :username :env
                              :password :env
                              :sign-releases false}
                 "releases" {:url "https://clojars.org/repo"
                             :username :env
                             :password :env
                             :sign-releases false}}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.onyxplatform/onyx "0.10.0"]
                 [clojure-future-spec "1.9.0-alpha17"]
                 [com.stuartsierra/dependency "0.2.0"]]
  :plugins [[codox "0.8.8"]])
