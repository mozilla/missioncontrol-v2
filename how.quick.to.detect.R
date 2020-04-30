#d <- dall.rel2[os=='Windows_NT',]
d <- dall.rel2


J <- function(M,D1){
    M <- label(M,'cci')
    rel.list <- list(cmr = M,ccr=M, cmi=M, cci=M)
    ll.rel <- make_posteriors(d1, CHAN='release', model.date = '2020-04-30',model.list=rel.list,last.model.date='2020-03-15')[modelname=='cci',]
    a <- merge(ll.rel[, list(model=mean(posterior)),by=list(os,c_version)][order(os,c_version),]
              ,d1[,list(actual=mean(cci)), by=list(os,c_version)], by=c("os","c_version"))
    a[order(os, c_version),]
}


d1 <- d[, head(.SD[order(date),],60),by=os]
d1 <- d1[,     ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
m1 <- make.a.model(d1,'cci',
                   debug=FALSE,
                   bff= bf( cci.logit   ~   os+  s(nvc,m=1) + (1+os|c_version),sigma~os)
                   )
(f1 <- J(m1,d1))



d1 <- d[, head(.SD[order(date),],61),by=os]
d1[os=='Windows_NT' & date==max(date), cci :=1.3*cci]
d1 <- d1[,     ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
m2 <- make.a.model(d1,'cci',
                   debug=FALSE,
                   bff= bf( cci.logit   ~   os+  s(nvc,m=1) + (1+os|c_version),sigma~os)
                   )
(f2 <- J(m2,d1))


d1 <- d[, head(.SD[order(date),],62),by=os]
d1[os=='Windows_NT' & date>=(max(date)-1), cci :=1.3*cci]
d1 <- d1[,     ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
m3 <- make.a.model(d1,'cci',
                   debug=FALSE,
                   bff= bf( cci.logit   ~   os+  s(nvc,m=1) + (1+os|c_version),sigma~os)
                   )
(f3 <- J(m3,d1))


d1 <- d[, head(.SD[order(date),],63),by=os]
d1[os=='Windows_NT' & date>=(max(date)-2), cci :=1.8*cci]
d1 <- d1[,     ":="(cmi.logit=boot::logit(cmi), cci.logit=boot::logit(cci))]
m4 <- make.a.model(d1,'cci',
                   debug=FALSE,
                   bff= bf( cci.logit   ~   os+  s(nvc,m=1) + (1+os|c_version),sigma~os)
                   )
(f4 <- J(m4,d1))

