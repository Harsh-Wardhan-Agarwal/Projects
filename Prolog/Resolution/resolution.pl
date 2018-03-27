%use_module(library(basics)).

:-import length/2 from basics.
:-import member/2 from basics.
:-import select/3 from basics.



append([],L2,L2).
append([H|T],L2,[H|L3]):-
	append(T,L2,L3).

simplify_clause([or(X,Y)], S):-
	atom(X),
	atom(Y),
	S = [X,Y].

simplify_clause([or(X,Y)], S):-
	X = neg(X1),
	atom(X1),
	atom(Y),
	S = [X,Y].

simplify_clause([or(X,Y)], S):-
	Y = neg(Y1),
	atom(X),
	atom(Y1),
	S = [X,Y].

simplify_clause([or(X,Y)], S):-
	X = neg(X1),
	Y = neg(Y1),
	atom(X1),
	atom(Y1),
	S = [X,Y].

simplify_clause([neg(X)], S):-
	atom(X),
	S = [neg(X)].

simplify_clause([X], S):-
	atom(X),
	S = [X].

simplify_clause([or(X,Y)], S):-
	atom(Y),
	simplify_clause([X], S2),
	append(S2,[Y],S).

simplify_clause([or(X,Y)], S):-
	Y = neg(Y1),
	atom(Y1),
	simplify_clause([X], S2),
	append(S2,[neg(Y)],S).


simplify([],[]).
simplify([H|T],L2):-
	H = [Index, Clause],
	simplify_clause(Clause, SClause),
	S = [Index,SClause],
	simplify(T,L3),
	append([S],L3,L2).


simplify_query(Q,L2,S):-
	atom(Q),
	length(L2,Len),
	Len1 is Len+1,
	S = [[Len1,[neg(Q)]]].

simplify_query(Q,L2,S):-
	Q = neg(Q1),
	atom(Q1),
	length(L2,Len),
	Len1 is Len+1,
	S = [[Len1,[Q1]]].


simplify_query(and(X,Y), L2, S):-
	atom(X),
	atom(Y),
	length(L2,Len),
	Len1 is Len+1,
	S = [[Len1, [neg(X),neg(Y)]]],
	!.

simplify_query(and(X,Y), L2, S):-
	X = neg(X1),
	Y = neg(Y1),
	atom(X1),
	atom(Y1),
	length(L2,Len),
	Len1 is Len+1,
	S = [[Len1, [X1,Y1]]],
	!.

simplify_query(and(X,Y), L2, S):-
	X = neg(X1),
	atom(X1),
	atom(Y),
	length(L2,Len),
	Len1 is Len+1,
	S = [[Len1, [X1,neg(Y)]]],
	!.

simplify_query(and(X,Y), L2, S):-
	Y = neg(Y1),
	atom(X),
	atom(Y1),
	length(L2,Len),
	Len1 is Len+1,
	S = [[Len1, [neg(X),Y1]]],
	!.


solve_list([],[]).
solve_list([H|T],S):-
	H = [N,C],
	solve_list(T,S2),
	append(C,S2,S3),
	append([N],S3,S4),
	sort(S4,S).


simplify_query(and(X,Y), L2, S):-
	atom(Y),
	length(L2,Len),
	Len1 is Len+1,
	simplify_query(X, L2, S2),
	append(S2,[[Len1,[neg(Y)]]],S5),
	solve_list(S5,S6),
	S6 = [N|T],
	S = [[N,T]],
	!.


simplify_query(and(X,Y), L2, S):-
	Y = neg(Y1),
	atom(Y1),
	length(L2,Len),
	Len1 is Len+1,
	simplify_query(X, L2, S2),
	append(S2,[[Len1,[Y1]]],S5),
	solve_list(S5,S6),
	S6 = [N|T],
	S = [N,T],
	!.


simplify_query(or(X,Y), L2, S):-
	atom(X),
	atom(Y),
	length(L2,Len),
	Len1 is Len+1,
	Len2 is Len1+1,
	S = [[Len1,[neg(X)]],[Len2,[neg(Y)]]].

simplify_query(or(X,Y), L2, S):-
	X = neg(X1),
	Y = neg(Y1),
	atom(X1),
	atom(Y1),
	length(L2,Len),
	Len1 is Len+1,
	Len2 is Len1+1,
	S = [[Len1,[X1]],[Len2,[Y1]]].

simplify_query(or(X,Y), L2, S):-
	X = neg(X1),
	atom(X1),
	atom(Y),
	length(L2,Len),
	Len1 is Len+1,
	Len2 is Len1+1,
	S = [[Len1,[X1]],[Len2,[neg(Y)]]].

simplify_query(or(X,Y), L2, S):-
	Y = neg(Y1),
	atom(X),
	atom(Y1),
	length(L2,Len),
	Len1 is Len+1,
	Len2 is Len1+1,
	S = [[Len1,[neg(X)]],[Len2,[Y1]]].

simplify_query(or(X,Y), L2, S):-
	atom(Y),
	length(L2,Len),
	Len1 is Len+1,
	S1 = [[Len1,[neg(Y)]]],
	append(L2,S1,L3),
	simplify_query(X, L3, S2),
	append(S1,S2,S).

simplify_query(or(X,Y), L2, S):-
	Y = neg(Y1),
	atom(Y1),
	length(L2,Len),
	Len1 is Len+1,
	S1 = [[Len1,[Y1]]],
	append(L2,S1,L3),
	simplify_query(X, L3, S2),
	append(S1,S2,S).


tautology(C):-
	member(Z,C),
	member(neg(Z),C),
	atom(Z).

process([],empty):-
	!.
process([X],X).
process([H1,H2|T],C):-
	HN = or(H1,H2),
	process([HN|T],C).


print([]).
print(Out):-
	Out = [H|T],
	H = [X,Y,E,Z],
	process(E,C),
	write('resolution('),
	write(X),
	write(', '),
	write(Y),
	write(', '),
	write(C),
	write(', '),
	write(Z),
	write(').'),
	writeln(''),
	print(T).
	

resolute(P,Len,Out):-
	member(Y1,P),
	member(Y2,P),
	Y1 = [Index1, C1],
	Y2 = [Index2, C2],
	member(X,C1),
	member(neg(X),C2),
	select(X,C1,NC1),
	select(neg(X),C2,NC2),
	append(NC1,NC2,C3),
	sort(C3,NC3),
	\+tautology(NC3),
	Len1 is Len + 1,
	Step = [Index1, Index2, NC3, Len1],
	append(Out,[Step],New_out),
	New = [Len1, NC3],
	append(P,[New],P1),
	select(Y1,P1,P2),
	select(Y2,P2,P3),
	length(NC3,Clen),
	(Clen =:= 0 ->
		writeln('resolution(success).'),
		print(New_out)
		;
		resolute(P3,Len1,New_out)
		).

read_file(Stream, Lines) :-
    (  at_end_of_stream(Stream) ->
		Lines=[]
    ;       
    	read(Stream, Line),
    	Lines = [Line|NewLines],
	    read_file(Stream, NewLines)
    ).


get_clauses([myQuery(_,_)],[]).
get_clauses([H|T],L):-
	H = myClause(X,Y),
	L1 = [[X,[Y]]],
	get_clauses(T,L2),
	append(L1,L2,L).

get_query([H],Q):-
	H = myQuery(X,Q).
get_query([H|T],Q):-
	get_query(T,Q).
	

resolution(File):-
	open(File, read, Str),
	read_file(Str, Lines),
	close(Str),
	get_clauses(Lines,L1),
	simplify(L1,L2),!,
	get_query(Lines,Q1),
	simplify_query(Q1,L2,SQ),!,
	append(L2,SQ,P),
	length(P,Len1),
	(\+resolute(P,Len1,[]) ->
		writeln('resolution(fail).')
		).
