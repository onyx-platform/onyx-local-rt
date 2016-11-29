(ns ^:no-doc onyx-local-rt.impl
  (:require [com.stuartsierra.dependency :as dep]
            [onyx.static.util :refer [kw->fn exception?]]
            [onyx.lifecycles.lifecycle-compile :as lc]
            [onyx.flow-conditions.fc-compile :as fc]
            [onyx.flow-conditions.fc-routing :as r]
            [onyx.windowing.window-extensions :as we]
            [onyx.windowing.aggregation]
            [onyx.refinements]
            [onyx.triggers]
            [onyx.types :refer [map->TriggerState]]
            [onyx.spec]))

(defn takev [k xs]
  (vec (take k xs)))

(defn dropv [k xs]
  (vec (drop k xs)))

(defn mapcatv [f xs]
  (vec (mapcat f xs)))

(defn curry-params [f params]
  (reduce partial f params))

(defn unqualify-map [m]
  (into {} (map (fn [[k v]] [(keyword (name k)) v]) m)))

(defn grouped-task? [task-map]
  (or (:onyx/group-by-key task-map)
      (:onyx/group-by-fn task-map)))

(defn only [coll]
  (when (next coll)
    (throw (ex-info "More than one element in collection, expected count of 1" {:coll coll})))
  (if-let [result (first coll)]
    result
    (throw (ex-info "Zero elements in collection, expected exactly one" {:coll coll}))))

(defn find-task [catalog task-name]
  (let [matches (filter #(= task-name (:onyx/name %)) catalog)]
    (only matches)))

(defn make-uuid []
  #?(:clj (java.util.UUID/randomUUID))
  #?(:cljs (random-uuid)))

(defn resolve-var [v]
  #?(:clj (var-get v))
  #?(:cljs v))

(defn resolve-aggregation-calls [s]
  (let [kw (if (sequential? s) (first s) s)]
    (resolve-var (kw->fn kw))))

(defn resolve-window-init [window calls]
  (if-not (:aggregation/init calls)
    (let [init (:window/init window)]
      (when-not init
        (throw (ex-info "No :window/init supplied, this is required for this aggregation" {:window window})))
      (constantly init))
    (:aggregation/init calls)))

(defn compile-window [window]
  (let [calls (resolve-aggregation-calls (:window/aggregation window))
        init-fn (resolve-window-init window calls)]
    {:window window
     :window-record ((we/windowing-builder window) (unqualify-map window))
     :resolved-aggregations calls
     :init-fn init-fn}))

(defn task-map->grouping-fn [task-map]
  (if-let [group-key (:onyx/group-by-key task-map)]
    (cond (keyword? group-key)
          group-key
          (sequential? group-key)
          #(select-keys % group-key)
          :else
          #(get % group-key))
    (if-let [group-fn (:onyx/group-by-fn task-map)]
      (kw->fn group-fn))))

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
                (assoc :compiled-after-apply-fn
                       (lc/compile-after-apply-fn-task-functions lifecycles task))
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

(defn windows->event-map
  [{:keys [onyx.core/windows onyx.core/task] :as event}]
  (let [compiled-windows
        (map
         compile-window
         (filter (fn [window] (= (:window/task window) task)) windows))]
    (update-in event [:onyx.core/compiled] assoc :windows compiled-windows)))

(defn resolve-trigger
  [{:keys [trigger/sync trigger/refinement trigger/on trigger/window-id] :as trigger}]
  (let [refinement-calls (resolve-var (kw->fn refinement))
        trigger-calls (resolve-var (kw->fn on))]
    (let [trigger (assoc trigger :trigger/id (make-uuid))
          f-init-state (:trigger/init-state trigger-calls)]
      (-> trigger
          (assoc :trigger trigger)
          (assoc :sync-fn (kw->fn sync))
          (assoc :state (f-init-state trigger))
          (assoc :init-state f-init-state)
          (assoc :next-trigger-state (:trigger/next-state trigger-calls))
          (assoc :trigger-fire? (:trigger/trigger-fire? trigger-calls))
          (assoc :create-state-update (:refinement/create-state-update refinement-calls))
          (assoc :apply-state-update (:refinement/apply-state-update refinement-calls))
          map->TriggerState))))

(defn live-triggers [windows triggers]
  (let [window-ids (into #{} (map :window/id windows))]
    (filter
     (fn [trigger]
       (some #{(:trigger/window-id trigger)} window-ids))
     triggers)))

(defn triggers->event-map
  [{:keys [onyx.core/windows onyx.core/triggers onyx.core/task-map] :as event}]
  (let [grouped? (grouped-task? task-map)
        live-triggers (live-triggers windows triggers)
        event (assoc-in event [:onyx.core/compiled :triggers] live-triggers)]
    (reduce
     (fn [event [{:keys [trigger/window-id] :as trigger} k]]
       (let [t (resolve-trigger trigger)
             f
             (if grouped?
               (fn [id window-state segment]
                 (let [group-f (get-in event [:onyx.core/compiled :grouping-fn])
                       group (group-f segment)]
                   (assoc-in window-state [id :trigger-states k group :trigger-state] t)))
               (fn [id window-state]
                 (assoc-in window-state [id :trigger-states k :trigger-state] t)))]
         (assoc-in event [:onyx.core/compiled :build-trigger-fn k] f)))
     event
     (map vector live-triggers (range)))))

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
   :lifecycle/apply-fn :lifecycle/after-apply-fn
   :lifecycle/after-apply-fn :lifecycle/route-flow-conditions
   :lifecycle/route-flow-conditions :lifecycle/assign-windows
   :lifecycle/assign-windows :lifecycle/fire-triggers
   :lifecycle/fire-triggers :lifecycle/write-batch
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

(defmethod apply-action :lifecycle/after-apply-fn
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-apply-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/after-read-batch
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-read-batch-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defn collect-next-segments [f input]
  (let [segments (try (f input)
                      (catch #?(:clj Throwable) #?(:cljs js/Error) e
                        (ex-info "Segment threw exception"
                                 {:exception e :segment input})))]
    (if (sequential? segments) segments (vector segments))))

(defmethod apply-action :lifecycle/apply-fn
  [env {:keys [event] :as task} action]
  (let [{:keys [onyx.core/batch onyx.core/params]} event]
    {:task
     (if (seq batch)
       (let [f (curry-params (:onyx.core/fn event) params)
             results (mapv
                      (fn [old]
                        (let [all-new (collect-next-segments f old)]
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
                  (when (and (exception? new-msg)
                             (not (seq (:flow routes))))
                    (throw new-msg))
                  (conj all* {:segment transformed-msg :routes (:flow routes)})))
              all
              all-new)))
         []
         results)]
    {:task (assoc-in task [:event :onyx.core/results] reified-results)}))

(defn apply-extents [window init-fn resolved-calls state extents segment]
  (let [state-f (:aggregation/create-state-update resolved-calls)
        update-f (:aggregation/apply-state-update resolved-calls)]
    (reduce
     (fn [result extent]
       (update-in result [:state extent]
                  (fn [state*]
                    (let [state** (or state* (init-fn window))
                          v (state-f window state** segment)]
                      (update-f window state** v)))))
     (assoc-in state [:state-event] {:event-type :new-segment
                                     :extents extents})
     extents)))

(defn state-transition-fns [event state segment]
  (if-let [f (get-in event [:onyx.core/compiled :grouping-fn])]
    (let [group (f segment)]
      {:state* (get state group)
       :ret-f (fn [v] (assoc state group v))})
    {:state* state
     :ret-f (fn [v] v)}))

(defn no-merge-next-window
  [{:keys [window window-record init-fn resolved-aggregations]} window-state event outgoing-segments]
  (reduce
   (fn [state {:keys [segment] :as msg}]
     (let [{:keys [state* ret-f]} (state-transition-fns event state segment)
           coerced (we/uniform-units window-record segment)
           extents (we/extents window-record state* coerced)
           result (apply-extents window init-fn resolved-aggregations state* extents coerced)]
       (ret-f result)))
   window-state
   outgoing-segments))

(defn merge-next-window
  [{:keys [window window-record init-fn resolved-aggregations]} window-state event outgoing-segments]
  (let [super-agg-f (:aggregation/super-aggregation-fn resolved-aggregations)]
    (reduce
     (fn [state {:keys [segment] :as msg}]
       (let [{:keys [state* ret-f]} (state-transition-fns event state segment)
             segment-coerced (we/uniform-units window-record segment)
             state* (we/speculate-update window-record state* segment-coerced)
             state* (we/merge-extents window-record state* super-agg-f segment-coerced)
             extents (we/extents window-record (keys state*) segment-coerced)
             result (apply-extents window init-fn resolved-aggregations state* extents segment-coerced)]
         (ret-f result)))
     window-state
     outgoing-segments)))

(defmulti next-window
  (fn [compiled-window window-state event outgoing-segments]
    (get-in compiled-window [:window :window/type])))

(defmethod next-window :fixed
  [compiled-window window-state event outgoing-segments]
  (no-merge-next-window compiled-window window-state event outgoing-segments))

(defmethod next-window :sliding
  [compiled-window window-state event outgoing-segments]
  (no-merge-next-window compiled-window window-state event outgoing-segments))

(defmethod next-window :global
  [compiled-window window-state event outgoing-segments]
  (no-merge-next-window compiled-window window-state event outgoing-segments))

(defmethod next-window :session
  [compiled-window window-state event outgoing-segments]
  (merge-next-window compiled-window window-state event outgoing-segments))

(defmethod apply-action :lifecycle/assign-windows
  [env {:keys [event] :as task} action]
  (let [{:keys [onyx.core/results]} event
        new-state
        (reduce
         (fn [window-state {:keys [window-record window] :as w}]
           (let [id (:window/id window)
                 old-state (get-in window-state [id :window-state])
                 next-state (next-window w old-state event results)]
             (assoc-in window-state [id :window-state] next-state)))
         (:onyx.core/window-states event)
         (get-in event [:onyx.core/compiled :windows]))]
    {:task (assoc-in task [:event :onyx.core/window-states] new-state)}))

(defn trigger-extent [window window-state state-event event-results]
  (let [{:keys [trigger-state extent]} state-event
        {:keys [sync-fn trigger create-state-update apply-state-update]} trigger-state
        extent-state (get (:state window-state) extent)
        state-event (assoc state-event :extent-state extent-state)
        entry (create-state-update trigger extent-state state-event)
        new-extent-state (apply-state-update trigger extent-state entry)
        state-event (-> state-event
                        (assoc :next-state new-extent-state)
                        (assoc :trigger-update entry))]
    (sync-fn (:task-event state-event) window trigger state-event extent-state)
    {:window-state (assoc-in window-state [:state extent] new-extent-state)
     :event-results (if (= extent-state new-extent-state)
                      event-results
                      (conj event-results state-event))}))

(defn trigger [window window-record window-state state-event event-results]
  (let [{:keys [trigger-index trigger-state]} state-event
        {:keys [trigger next-trigger-state trigger-fire? fire-all-extents?]} trigger-state
        old-trigger-state (:state trigger-state)
        state-event (assoc state-event :window window)
        new-trigger-state (next-trigger-state trigger old-trigger-state state-event)
        fire-all? (or fire-all-extents? (not= (:event-type state-event) :new-segment))
        fire-extents (if fire-all? (keys window-state) (:extents state-event))]
    (reduce
     (fn [t extent]
       (let [[lower-bound upper-bound] (we/bounds window-record extent)
             state-event (-> state-event
                             (assoc :lower-bound lower-bound)
                             (assoc :upper-bound upper-bound)
                             (assoc :extent extent))]
         (if (trigger-fire? trigger new-trigger-state state-event)
           (let [rets (trigger-extent window window-state state-event event-results)]
             (assoc t :window-state (:window-state rets)))
           t)))
     {:trigger-state (assoc trigger-state :state new-trigger-state)
      :window-state window-state}
     fire-extents)))

(defn build-trigger-state [event window-id window-state results]
  (let [grouping-f (get-in event [:onyx.core/compiled :grouping-fn])
        k->f (get-in event [:onyx.core/compiled :build-trigger-fn])]
    (cond (and (not grouping-f) (get-in window-state [window-id :trigger-states]))
          window-state

          (not grouping-f)
          (reduce-kv (fn [state k f] (f window-id state)) window-state k->f)

          :else
          (reduce
           (fn [state {:keys [segment]}]
             (let [group (grouping-f segment)]
               (if (get-in state [window-id :trigger-states 0 group])
                 state
                 (reduce-kv (fn [state* k f] (f window-id state* segment)) state k->f))))
           window-state
           results))))

(defn update-trigger-ungrouped [event old-state window window-record results]
  (reduce-kv
   (fn [result trigger-index {:keys [trigger-state]}]
     (if (= (get-in result [(:window/id window) :window-state :state-event :event-type])
            :new-segment)
       (let [state-event
             (merge
              (get-in result [(:window/id window) :window-state :state-event])
              {:log-type :trigger
               :trigger-index trigger-index
               :trigger-state trigger-state})
             {:keys [window/id]} window
             window-state (get-in result [(:window/id window) :window-state])
             rets (trigger window window-record window-state state-event results)]
         (-> result
             (assoc-in [id :trigger-states trigger-index :trigger-state] (:trigger-state rets))
             (assoc-in [id :window-state] (:window-state rets))))
       result))
   old-state
   (get-in old-state [(:window/id window) :trigger-states])))

(defn update-trigger-grouped [event old-state window window-record results]
  (let [group-f (get-in event [:onyx.core/compiled :grouping-fn])]
    (reduce
     (fn [state {:keys [segment] :as result}]
       (let [group (group-f segment)]
         (reduce-kv
          (fn [state* trigger-index groups]
            (let [{:keys [trigger-state]} (get groups group)]
              (if (= (get-in state* [(:window/id window) :window-state
                                     group :state-event :event-type])
                     :new-segment)
                (let [state-event
                      (merge
                       (get-in state* [(:window/id window) :window-state
                                       group :state-event])
                       {:log-type :trigger
                        :trigger-index trigger-index
                        :trigger-state trigger-state
                        :grouped? true
                        :group group})
                      {:keys [window/id]} window
                      window-state (get-in state* [id :window-state group])
                      rets (trigger window window-record window-state state-event results)]
                  (-> state*
                      (assoc-in [id :trigger-states trigger-index group :trigger-state] (:trigger-state rets))
                      (assoc-in [id :window-state group] (:window-state rets))))
                state*)))
          state
          (get-in old-state [(:window/id window) :trigger-states]))))
     old-state
     results)))

(defmethod apply-action :lifecycle/fire-triggers
  [env {:keys [event] :as task} action]
  (let [{:keys [onyx.core/results onyx.core/compiled]} event
        grouped? (:grouping-fn compiled)]
    (if (seq results)
      (let [new-state
            (reduce
             (fn [window-states {:keys [window window-record]}]
               (let [window-id (:window/id window)
                     old-state (build-trigger-state event window-id window-states results)
                     update-f (if grouped? update-trigger-grouped update-trigger-ungrouped)]
                 (update-f event old-state window window-record results)))
             (:onyx.core/window-states event)
             (get-in event [:onyx.core/compiled :windows]))]
        {:task (assoc-in task [:event :onyx.core/window-states] new-state)})
      {:task task})))

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
  [{:keys [workflow catalog flow-conditions lifecycles windows triggers] :as job}
   graph task-name catalog-entry]
  (let [children (into #{} (dep/immediate-dependents graph task-name))
        base {:inbox []
              :start-task? false
              :children children
              :event (-> {:onyx.core/task task-name
                          :onyx.core/workflow workflow
                          :onyx.core/catalog catalog
                          :onyx.core/lifecycles lifecycles
                          :onyx.core/flow-conditions flow-conditions
                          :onyx.core/windows windows
                          :onyx.core/triggers triggers
                          :onyx.core/task-map catalog-entry
                          :onyx.core/fn (precompile-onyx-fn catalog-entry)
                          :onyx.core/compiled
                          {:grouping-fn (task-map->grouping-fn catalog-entry)}}
                         (lifecycles->event-map)
                         (flow-conditions->event-map)
                         (windows->event-map)
                         (triggers->event-map)
                         (task-params->event-map)
                         (egress-ids->event-map children))}]
    (if (seq children)
      {task-name base}
      {task-name (assoc base :outputs [])})))

(defn init-task-states [{:keys [workflow catalog] :as job} graph]
  (let [tasks (reduce into #{} workflow)]
    (apply merge
           (map
            (fn [task-name]
              (let [catalog-entry (find-task catalog task-name)]
                (init-task-state job graph task-name catalog-entry)))
            tasks))))

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
