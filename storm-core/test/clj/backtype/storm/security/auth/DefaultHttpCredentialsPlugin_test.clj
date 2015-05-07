(ns backtype.storm.security.auth.DefaultHttpCredentialsPlugin-test
  (:require [clojure.test :refer :all])
  (:import [javax.security.auth Subject]
           [javax.servlet.http HttpServletRequest]
           [backtype.storm.security.auth SingleUserPrincipal]
           [org.mockito Mockito]
           [backtype.storm.security.auth DefaultHttpCredentialsPlugin
                                         ReqContext SingleUserPrincipal]))

(deftest test-getUserName
  (let [handler (doto (DefaultHttpCredentialsPlugin.) (.prepare {}))]
    (testing "returns null when request is null"
      (is (nil? (.getUserName handler nil))))

    (testing "returns null when user principal is null"
      (let [req (Mockito/mock HttpServletRequest)]
        (is (nil? (.getUserName handler req)))))

    (testing "returns null when user is blank"
      (let [princ (SingleUserPrincipal. "")
            req (Mockito/mock HttpServletRequest)]
        (. (Mockito/when (. req getUserPrincipal))
           thenReturn princ)
        (is (nil? (.getUserName handler req)))))

    (testing "returns correct user from requests principal"
      (let [exp-name "Alice"
            princ (SingleUserPrincipal. exp-name)
            req (Mockito/mock HttpServletRequest)]
        (. (Mockito/when (. req getUserPrincipal))
           thenReturn princ)
        (is (.equals exp-name (.getUserName handler req)))))

    (testing "returns doAsUser from requests principal when Header has doAsUser param set"
      (let [exp-name "Alice"
            do-as-user-name "Bob"
            princ (SingleUserPrincipal. exp-name)
            req (Mockito/mock HttpServletRequest)
            _ (. (Mockito/when (. req getUserPrincipal))
              thenReturn princ)
            _ (. (Mockito/when (. req getHeader "doAsUser"))
              thenReturn do-as-user-name)
            context (.populateContext handler (ReqContext/context) req)]
        (is (= true (.isImpersonating context)))
        (is (.equals exp-name (.getName (.realPrincipal context))))
        (is (.equals do-as-user-name (.getName (.principal context))))))))

(deftest test-populate-req-context-on-null-user
  (let [req (Mockito/mock HttpServletRequest)
        handler (doto (DefaultHttpCredentialsPlugin.) (.prepare {}))
        subj (Subject. false (set [(SingleUserPrincipal. "test")]) (set []) (set []))
        context (ReqContext. subj)]
    (is (= 0 (-> handler (.populateContext context req) (.subject) (.getPrincipals) (.size))))))
