(ns onyx-local-rt.core
  (:require [com.stuartsierra.dependency :as dep]
            [onyx.static.util :refer [kw->fn]]
            [onyx.static.planning :refer [find-task]]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.flow-conditions.fc-compile :as fc]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.peer.transform :as t]))

;;; Functions for example

(defn my-inc [segment]
  (update-in segment [:n] inc))

(defn segment-even? [event old new all-new]
  (even? (:n new)))

;;;

(defn takev [k xs]
  (vec (take k xs)))

(defn dropv [k xs]
  (vec (drop k xs)))

(defn mapcatv [f xs]
  (vec (mapcat f xs)))

(defn lifecycles->event-map
  [{:keys [onyx.core/lifecycles onyx.core/task] :as event}]
  (update event
          :onyx.core/compiled
          (fn [compiled]
            (-> compiled
                (assoc :compiled-start-task-fn
                       (lc/compile-start-task-functions lifecycles task))
                (assoc :compiled-before-task-start-fn
                       (lc/compile-before-task-start-functions lifecycles task))
                (assoc :compiled-before-batch-fn
                       (lc/compile-before-batch-task-functions lifecycles task))
                (assoc :compiled-after-read-batch-fn
                       (lc/compile-after-read-batch-task-functions lifecycles task))
                (assoc :compiled-after-batch-fn
                       (lc/compile-after-batch-task-functions lifecycles task))
                (assoc :compiled-after-task-fn
                       (lc/compile-after-task-functions lifecycles task))
                (assoc :compiled-handle-exception-fn
                       (lc/compile-handle-exception-functions lifecycles task))))))

(defn flow-conditions->event-map
  [{:keys [onyx.core/flow-conditions onyx.core/workflow onyx.core/task] :as event}]
  (update event
          :onyx.core/compiled
          (fn [compiled]
            (-> compiled
                (assoc :flow-conditions flow-conditions)
                (assoc :compiled-norm-fcs (fc/compile-fc-happy-path flow-conditions workflow task))
                (assoc :compiled-ex-fcs (fc/compile-fc-exception-path flow-conditions workflow task))))))

(defn task-params->event-map [{:keys [onyx.core/task-map] :as event}]
  (let [params (map (fn [param] (get task-map param))
                    (:onyx/params task-map))]
    (assoc event :onyx.core/params params)))

(defn egress-ids->event-map [event children]
  (assoc-in event [:onyx.core/compiled :egress-ids] children))

(def action-sequence
  {:lifecycle/start-task? :lifecycle/before-task-start
   :lifecycle/before-task-start :lifecycle/before-batch
   :lifecycle/before-batch :lifecycle/read-batch
   :lifecycle/read-batch :lifecycle/after-read-batch 
   :lifecycle/after-read-batch :lifecycle/apply-fn
   :lifecycle/apply-fn :lifecycle/route-flow-conditions
   :lifecycle/route-flow-conditions :lifecycle/write-batch
   :lifecycle/write-batch :lifecycle/after-batch
   :lifecycle/after-batch :lifecycle/before-batch
   :lifecycle/after-task-stop :lifecycle/start-task?})

(defmulti apply-action
  (fn [env task action]
    action))

(defmethod apply-action :lifecycle/start-task?
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-start-task-fn])]
    {:task (assoc task :start-task? (f (:event task)))}))

(defmethod apply-action :lifecycle/before-task-start
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-before-task-start-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/before-batch
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-before-batch-fn])
        event (dissoc (:event task) :onyx.core/batch :onyx.core/results)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/read-batch
  [env {:keys [inbox event] :as task} action]
  (let [size (:onyx/batch-size (:onyx.core/task-map event))]
    {:task
     (-> task
         (assoc-in [:event :onyx.core/batch] (takev size inbox))
         (assoc :inbox (dropv size inbox)))}))

(defmethod apply-action :lifecycle/after-read-batch
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-read-batch-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/apply-fn
  [env {:keys [event] :as task} action]
  (let [{:keys [onyx.core/batch onyx.core/params]} event]
    {:task
     (if (seq batch)
       (let [f (t/curry-params (:onyx.core/fn event) params)
             results (mapv
                      (fn [old]
                        (let [all-new (t/collect-next-segments f old)]
                          {:old old :all-new all-new}))
                      batch)]
         (assoc-in task [:event :onyx.core/results] results))
       task)}))

(defmethod apply-action :lifecycle/route-flow-conditions
  [env {:keys [event] :as task} action]
  (let [{:keys [onyx.core/results onyx.core/compiled]} event
        reified-results
        (reduce
         (fn [all {:keys [old all-new] :as outgoing-message}]
           (let [leaves (mapv (partial hash-map :message) all-new)
                 root {:message old}]
             (reduce
              (fn [all* new-msg]
                (let [routes (r/route-data event compiled {:root root :leaves leaves} new-msg)
                      transformed-msg (r/flow-conditions-transform new-msg routes event compiled)]
                  (conj all* {:segment transformed-msg :routes (:flow routes)})))
              all
              all-new)))
         []
         results)]
    {:task (assoc-in task [:event :onyx.core/results] reified-results)}))

(defn route-to-children [results]
  (reduce
   (fn [result {:keys [segment routes]}]
     (reduce
      (fn [result* route]
        (update-in result* [route] (fnil conj []) segment))
      result
      routes))
   {}
   results))

(defmethod apply-action :lifecycle/write-batch
  [env {:keys [event children] :as task} action]
  (let [{:keys [onyx.core/results]} event]
    (cond (not (seq children))
          {:task (update-in task [:outputs] into (mapv :segment results))
           :writes {}}

          (seq results)
          {:task task
           :writes (route-to-children results)}

          :else
          {:task task
           :writes {}})))

(defmethod apply-action :lifecycle/after-batch
  [env task action]
  (let [event (:event task)
        f (get-in event [:onyx.core/compiled :compiled-after-batch-fn])]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/after-task-stop
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-task-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defn workflow->sierra-graph [workflow]
  (reduce
   (fn [result [src dst]]
     (dep/depend result dst src))
   (dep/graph)
   workflow))

(defn precompile-onyx-fn [catalog-entry]
  (if-let [f (:onyx/fn catalog-entry)]
    (kw->fn f)
    clojure.core/identity))

(defn init-task-state
  [graph lifecycles flow-conditions task-name catalog-entry]
  (let [children (into #{} (dep/immediate-dependents graph task-name))
        base {:inbox []
              :start-task? false
              :children children
              :event (-> {:onyx.core/task task-name
                          :onyx.core/lifecycles lifecycles
                          :onyx.core/flow-conditions flow-conditions
                          :onyx.core/task-map catalog-entry
                          :onyx.core/fn (precompile-onyx-fn catalog-entry)}
                         (lifecycles->event-map)
                         (flow-conditions->event-map)
                         (task-params->event-map)
                         (egress-ids->event-map children))}]
    (if (seq children)
      {task-name base}
      {task-name (assoc base :outputs [])})))

(defn init-task-states [workflow catalog lifecycles flow-conditions graph]
  (let [tasks (reduce into #{} workflow)]
    (apply merge
           (map
            (fn [task-name]
              (let [catalog-entry (find-task catalog task-name)]
                (init-task-state graph lifecycles flow-conditions
                                 task-name catalog-entry)))
            tasks))))

(defn init [{:keys [workflow catalog lifecycles flow-conditions] :as job}]
  (let [graph (workflow->sierra-graph workflow)]
    {:tasks (init-task-states workflow catalog lifecycles flow-conditions graph)
     :sorted-tasks (dep/topo-sort graph)
     :pending-writes {}
     :next-action :lifecycle/start-task?}))

(defn integrate-task-updates [env action]
  (reduce
   (fn [result task-name]
     (let [task-state (get-in env [:tasks task-name])
           rets (apply-action env task-state action)
           merge-f (partial merge-with into)]
       (-> result
           (assoc-in [:tasks task-name] (:task rets))
           (update-in [:pending-writes] merge-f (:writes rets)))))
   env
   (:sorted-tasks env)))

(defn transfer-pending-writes [env]
  (let [writes (:pending-writes env)]
    (reduce-kv
     (fn [result task-name segments]
       (update-in result [:tasks task-name :inbox] into segments))
     (assoc env :pending-writes {})
     writes)))

(defn transition-action-sequence [env action]
  (if (and (= action :lifecycle/start-task?)
           (not (every? true? (map :start-task? (vals (:tasks env))))))
    (assoc env :next-action :lifecycle/start-task?)
    (assoc env :next-action (action-sequence action))))

(defn tick [env]
  (let [this-action (:next-action env)]
    (-> env
        (integrate-task-updates this-action)
        (transfer-pending-writes)
        (transition-action-sequence this-action))))

(defn drained? [env]
  (let [task-states (vals (:tasks env))
        inboxes (map :inbox task-states)
        batches (map (comp :onyx.core/batch :event) task-states)]
    (and (every? (comp not seq) inboxes)
         (every? nil? batches))))

(defn drain
  ([env] (drain env 10000))
  ([env max-ticks]
   (loop [env env
          i 0]
     (cond (> i max-ticks)
           (throw (ex-info (format "Ticked %s times and never drained, runtime will not proceed with further execution." max-ticks) {}))

           (drained? env) env

           :else (recur (tick env) (inc i))))))

(defn env-summary [env]
  {:next-action (:next-action env)
   :tasks
   (reduce
    (fn [result task-name]
      (let [tm (get-in env [:tasks task-name :event :onyx.core/task-map])
            inbox (get-in env [:tasks task-name :inbox])]
        (if (= (:onyx/type tm) :output)
          (let [outputs (get-in env [:tasks task-name :outputs])]
            (assoc result task-name {:inbox inbox :outputs outputs}))
          (assoc result task-name {:inbox inbox}))))
    {}
    (keys (:tasks env)))})

(defmulti transition-env
  (fn [env action-data]
    (:event action-data)))

(defmethod transition-env :new-segment
  [env {:keys [task segment]}]
  (update-in env [:tasks task :inbox] conj segment))

(defmethod transition-env :stop
  [env action-data]
  (let [this-action :lifecycle/after-task-stop]
    (-> env
        (integrate-task-updates this-action)
        (transition-action-sequence this-action))))

(defn new-segment [env input-task segment]
  (transition-env env {:event :new-segment
                       :task input-task
                       :segment segment}))

(defn stop [env]
  (transition-env env {:event :stop}))

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 1}
             {:onyx/name :inc
              :onyx/type :function
              :onyx/fn ::my-inc
              :onyx/batch-size 1}
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 1}]
   :lifecycles []
   :flow-conditions
   [{:flow/from :inc
     :flow/to [:out]
     :flow/predicate ::segment-even?}]})

(clojure.pprint/pprint
 (-> (init job)
     (new-segment :in {:n 41})
     (new-segment :in {:n 84})
     (drain)
     (stop)
     (env-summary)))
