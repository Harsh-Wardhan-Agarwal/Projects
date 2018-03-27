append_list([],L,L):-
	!.
append_list([H|T],L2,[H|L3]):-
	append_list(T,L2,L3).


delete([X|Ys],X,Ys).
delete([Y|Ys],X,[Y|Zs]):-
	delete(Ys,X,Zs).

permute([],[]).
permute([X|Xs],Ys):-
	permute(Xs,Zs),
	delete(Ys,X,Zs).


location_list(0,_,[]):-
	!.
location_list(X,I,Y):-
	X1 is X-1,
	I1 is I+1,
	location_list(X1,I1,L),
	append_list([I1],L,Y).

violations(C):-
	places(X),
	location_list(X,0,Y),
	people(K),
	combinations(Y,K,C),
	!.

combinations(Y,K,C):-
	findall(L2,permute(Y,L2),L3),
	start(L3,K,C).

start([],_,9999).
start([H|T],K,Ret):-
	get_lists(K,H,S),
	start(T,K,C),
	min(C,S,Ret).

min(X,Y,X):-
	X=<Y.
min(Y,X,X):-
	X<Y.


trans_check(_,0,P,0):-
	!.
trans_check(K,M,P,S):-
	M1 is M-1,
	transitivity(K,M,L1,P,C),
	trans_check(K,M1,P,S1),
	S is C+S1.

transitivity(K,M,L,P,C):-
	findall(X,transitivity_check(K,M,X),L),
	violation_check(P,M,L,C).

violation_check([],M,L,0):-
	!.
violation_check([H|T],M,L,C):-
	( M=:=H -> tail_check(T,L,C) ;violation_check(T,M,L,C) ).

tail_check(T,[],0).
tail_check(T,[H2|T2],C1):-
	search(T,H2,C),
	tail_check(T,T2,C2),
	C1 is C+C2.



search([],[],0).
search([],H2,C1):-
	search([],T1,C),
	C1 is C+1.
search([H1|T1],H2,C):-
	(H1 =:= H2 -> C is 0; search(T1,H2,C)).



transitivity_check(K,M,X):-
	order(K,M,X).

transitivity_check(K,M,Y):-
	order(K,M,X),
	transitivity_check(K,X,Y).



get_lists(0,P,0):-
	!.
get_lists(K,P,SS):-
	places(M),
	trans_check(K,M,P,S),
	K1 is K-1,
	get_lists(K1,P,SS2),
	SS is S+SS2.
