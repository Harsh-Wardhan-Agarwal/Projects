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

get_dishes(0,[],_):-
	!.
get_dishes(N,L,I):-
	N1 is N-1,
	I1 is I+1,
	get_dishes(N1,L1,I1),
	append_list([I1],L1,L).

demand_list([],[]):-
	!.
demand_list([H|T],L1):-
	demand(H,X),
	demand_index(H,X,L),
	demand_list(T,L2),
	append_list(L,L2,L1).

demand_index(_,0,[]):-
	!.
demand_index(H,X,L3):-
	X1 is X-1,
	demand_index(H,X1,L1),
	append_list([H],L1,L3).

table_count([], 1,_,_) :- !.
table_count([H|T], R, S, P) :-
	
	table_width(W),
	seperation(K),
	hot(M),
	dish_width(H,X),

	(P = 0 ->
        Width is X
    ;   (P=<M, H=<M ->
            Width is X
        ;   (P>M, H>M ->
            	Width is X
        	;   Width is X + K
        	)
        )
    	
    ),
	(Width =< S ->
		Sp is S-Width,
		table_count(T, Ret, Sp, H),
		R is Ret
		;
		Sp is W-X,
		table_count(T, Ret, Sp, H),
		R is Ret+1
		).

min(A,B,B):-
	B=<A.
min(A,B,A):-
	A<B.

target([],999):- !.
target([H|T],B):-
	table_width(W),
	table_count(H,N,W,0),
	target(T,A),
	min(N,A,B).

tables(Ans):-
	dishes(D),
	get_dishes(D,L,0),
	demand_list(L,L1),
	findall(L2,permute(L1,L2),L3),
	target(L3,Ans).



