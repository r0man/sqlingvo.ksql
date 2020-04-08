(ns sqlingvo.ksql.error
  (:require [cheshire.core :as json]
            [sqlingvo.ksql.utils :refer [parse-json]]
            [clojure.string :as str]))

(defn- error-type
  "Returns the error type from the `response`."
  [response]
  (when-let [type (get-in response [:body (keyword "@type")])]
    (keyword "sqlingvo.ksql.error" (str/replace (name type) #"_" "-"))))

(defn- read-response
  "Read the HTTP `response`."
  [{:keys [body] :as response}]
  (cond
    (map? body)
    response
    (instance? java.io.InputStream body)
    (update response :body #(parse-json (slurp %)))
    :else {}))

(defn- unexceptional-status-data
  "Returns the unexceptional status data."
  [statement {:keys [body] :as response}]
  (let [{:keys [error_code message stackTrace]} body
        type (get body (keyword "@type"))]
    {:code error_code
     :message message
     :stack-trace stackTrace
     :statement statement
     :type (error-type response)}))

(defn- error-name
  "Returns the human readable error `type`."
  [type]
  (if type (str/capitalize (str/replace (name type) #"-" " ")) "n/a"))

(defn- error-message
  "Returns the human readable error message."
  [{:keys [type message]}]
  (str "SQLINGVO.KSQL Error: " ( error-name type) "\n\n" message))

(defn unexceptional-status
  "Returns the unexceptional exception info."
  [statement exception]
  (let [{:keys [type message] :as data}
        (unexceptional-status-data
         statement (read-response (ex-data exception)))]
    (ex-info (error-message data) data)))
