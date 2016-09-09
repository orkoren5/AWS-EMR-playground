hello world world hello hello


LYER1:

MAPEER input:
w1 w2 w3 w4 w5	year amount ignore ignore

MAPEER OUTPUT:			//disclmer - evry line is one year only!
<"decade *",decade_amount> 
<"decade wi *",One_word_amount>
<"decade wk wj",Pair_amount>
   
Reducer INPUT
<"decade *", decade_amount>			//disclmer - evry line is multi year!
<"decade wi *", One_word_amount1,One_word_amount2,One_word_amount2,...,One_word_amountN>
<"decade wk wj", Pair_amount1,Pair_amount2,Pair_amount3,...,Pair_amountN>

Reducer OUTPUT:
<"decade *", decade_amount>
<"decade wi *", One_word_amount1 + One_word_amount2 +One_word_amount2 +...+ One_word_amountN>
<"decade wk wj", Pair_amount1 + Pair_amount2 + Pair_amount3 +...+ Pair_amountN>

LYER2:

MAPEER INPUT -> OUTPUT
<"decade wk wj",Pair amount>			-> <"decade wk", "wj pairAmount">
<"decade wk *",one word amount>			-> <"decade wk", "* singleWordAmount">
<"decade *",all words in decade amount>		-> <"decade *",all words in decade amount>

Reducer INPUT -> OUTPUT
<"decade wk",("pairAmount_w1",......,"pairAmount_wN","singleWk")>	-> <"decade w1","wk pairAmount_w1_wk AmountOf_wk_asFirstWord">,.....
<"decade *",all words in decade amount>					->unchanged


LAY3:

MAPEER INPUT -> OUTPUT
<"decade w1","wk pairAmount_w1_wk single_wk">   ->?????????????
<"decade *",all words in decade amount>		-> unchanged

Reducer INPUT -> OUTPUT
			?????????					-> <"decade w1","wk pairAmount_w1_wk single_wk single_w1">,.....
<"decade *",all words in decade amount>					->unchanged

LAY4:

MAPEER INPUT -> OUTPUT
??????????????????

Reducer INPUT -> OUTPUT
		??????????->  <"decade w1","wk pairAmount_w1_wk single_wk single_w1 all_words_in_decade_amount ">

לk
יחי

