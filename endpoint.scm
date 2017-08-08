;; still a big mess, don't look closely

(use s-sparql s-sparql-parser)

(include "triples.scm")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; endpoint
(define (continuel block bindings)
  (with-rewrite ((rw (rewrite (cdr block) bindings)))
    rw))
(define (extract-all-variables where-clause)
  (delete-duplicates (filter sparql-variable? (flatten where-clause))))

(define (binding-or-val v bindings)
  (if (sparql-variable? v)
      (alist-ref v bindings)
      v))

(define always (== #t #t))

(define (rewriteo block #!optional (bindings '()) (rules (*rules*)))
  (rewrite* block bindings rules conj always))

(define (%query-rules delta)
  `((,select? . ,rw/remove)
    ((@Dataset) . ,rw/remove)
    (,triple? 
     . ,(lambda (triple bindings)
          (match triple
            ((s p o) 
             (let* ((C (lambda (#!rest vs) (length (filter (lambda (x) (and (not x) 1)) vs))))
                    (val-or-var (lambda (s) (if (sparql-variable? s)
                                                (get-binding s 'var bindings)
                                                s)))
                    (c (get-binding 'var-count bindings))
                    (sv* (val-or-var s))
                    (sv (or sv* (vector c)))
                    (pv* (val-or-var p))
                    (pv (or pv* (vector (+ c (C sv*)))))
                    (ov* (val-or-var o))
                    (ov (or ov* (vector (+ c (C sv* pv*))))))
               (values (triple-nol delta sv pv ov)
                       (update-bindings (s 'var sv) (p 'var pv) (o 'var ov)
                                        ('var-count (+ (get-binding 'var-count bindings)
                                                       (C sv* pv* ov*)))
                                        ('goals (cons `(,sv ,pv ,ov) (get-binding 'goals bindings)))
                                        bindings)))))))
    ;; OPTIONAL UNION GRAPH ...
    (,pair? . ,(lambda (block bindings)
                 (let-values (((rw _) (rewrite (cdr block) bindings expand-rules)))
                   (rewriteo rw bindings))))))

(define (run-query vars quads bindings)
  (let-values (((goal new-bindings) 
                (rewrite quads (update-bindings ('var-count 0) ('goals '()) bindings)
                         (%query-rules #(-1)))))
    (let* (($ (goal empty-state))
           (results (current $))
           (cont (promised $)))
      (values
       ;; integrate this with reify...
       (map (lambda (state)
              (cons (if (equal? (alist-ref #(-1) state) '-) '- '+)
              (map (lambda (var)
                     (let ((v (get-binding var 'var new-bindings)))
                       (cons var (alist-ref v state))))
                   vars)))
            (map car results)) 
       (update-binding 'promised cont bindings)))))

(define (%unit-rules)
  `(((@Query) 
     . ,(lambda (block bindings)
          (let ((vars (alist-ref 'SELECT (cdr block))))
            (run-query vars (cdr block) bindings))))
    ((@Update) 
     . ,(lambda (block bindings)
          (let* ((where (assoc 'WHERE (cdr block)))
                 (vars (extract-all-variables where)))
            (let-values (((results new-bindings) (if where
                                                     (run-query vars (list where) bindings)
                                                     (values '() '()))))
              (let-values (((updates _) (rewrite (cdr block) new-bindings (%update-rules results))))
                (let ((inserts (or (alist-ref 'INSERT updates) '()))
                      (deletes (or (alist-ref 'DELETE updates) '())))
                  (add-triples inserts)
                  (delete-triples deletes)
                  (values `((deletes . ,(length deletes))
                            (inserts . ,(length inserts)))
                          bindings)))))))
    (,pair? . ,continuel)))

(define (%update-rules results)
  `((,triple?
     . ,(lambda (triple bindings)
          (values
           (match triple
             ((s p o)
              (map (lambda (result)
                     (list (binding-or-val s result)
                           (binding-or-val p result)
                           (binding-or-val o result)))
                   results)))
           bindings)))
    ((WHERE @Using) . ,rw/remove)
    ((DELETE)
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings expand-rules)))
            (values `((DELETE ,@(rewrite rw bindings))) bindings))))
    ((INSERT |INSERT DATA|)
     . ,(lambda (block bindings)
          (let-values (((rw _) (rewrite (cdr block) bindings expand-rules)))
            (values `((INSERT ,@(rewrite rw bindings))) bindings))))))

(define expand-rules
  `((,triple? . ,(lambda (triple bindings)
                   (values (expand-triple triple) bindings)))
    (,pair? . ,rw/copy)))

(define q  "SELECT ?p ?o WHERE { <S> ?p ?o }")

(define up "INSERT { ?s ?p <ooooh> } WHERE { ?s ?p <O> }")

(define up2 "INSERT DATA { <S> a <JonnyTree> }")
          
(define (test1) (rewrite q (update-binding 'query-string q '()) (%unit-rules)))

(define (test2) (rewrite up '() (%unit-rules)))

(define (go qs)
  (let ((q (parse-query qs)))
    (rewrite q (update-binding 'query-string qs '()) (%unit-rules))))

