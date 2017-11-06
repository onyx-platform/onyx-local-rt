(defproject org.onyxplatform/onyx-local-rt "0.12.0.0-SNAPSHOT"
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
  :dependencies [[org.clojure/clojure "1.9.0-alpha20"]
                 ^{:voom {:repo "git@github.com:onyx-platform/onyx.git" :branch "master"}}
                 [org.onyxplatform/onyx "0.12.0-20171106_181548-ga4461bd"]
                 [org.onyxplatform/onyx-spec "0.11.0.0-alpha3"]
                 [com.stuartsierra/dependency "0.2.0"]]
  :plugins [[codox "0.8.8"]
            [lein-set-version "0.4.1"]
            [lein-update-dependency "0.1.2"]
            [lein-pprint "1.1.1"]])
