# EBNF文法
# 已弃用的文法，会导致解析冲突
# lite_ebnf = r"""
#     %import common.CNAME
#     %import common.WORD
#     %import common.NUMBER
#     %ignore WS
#     %import common.WS
#
#     STRING: /"[^"]*"/ | /'[^']*'/
#     COMMENT: /#.*/
#
#     %ignore COMMENT
#
#     start: define_source_file  model
#     CONST_VALUE: "$" CNAME
#     str_token: CNAME | NUMBER | WORD | STRING | CONST_VALUE
#     list: "("  [ str_token  ( ","  str_token )* ]  ")"
#     normal_node: CNAME [ list ]
#     apply_concurrency_node: [ extend_info_list ] "<" node  ( ","  node )* ">"
#     extend_info_list: "|" str_token  ("," str_token)* "|"
#     map_concurrency_node: [ extend_info_list ] ":" node
#     model: CNAME "[" link "]"
#     ref: "&" CNAME
#     node: [ CNAME "=" ] (normal_node | model | apply_concurrency_node | map_concurrency_node | choice | redo | while_loop | ref)
#     link: node ("->" node)*
#     file_reg_kv: (CNAME [ ":" CNAME ])
#     file_reg_kv_list: "[" file_reg_kv ("," file_reg_kv)* "]"
#     parma_kv: "=" STRING [ "(" CNAME ")" ]
#     no_parma: "!"
#     context_list: CNAME ("," CNAME)*
#     define_source_file: "@" [ file_reg_kv_list ] (parma_kv | no_parma | ("=" CONST_VALUE)) [ ":" context_list ]
#     choice: CNAME [ list ] "?:" "{" model ("," model)* "}"
#     while_loop: node "%" node "?"
#     redo: node "%" NUMBER
#     """

# 新的文法
ebnf =  r"""
    %import common.CNAME
    %import common.WORD
    %import common.NUMBER
    %ignore WS
    %import common.WS
    
    STRING: /"(?:\\.|[^"\\])*"/ | /'(?:\\.|[^'\\])*'/
    COMMENT: /#.*/

    %ignore COMMENT
    
    start: define_source_file model
    CONST_VALUE: "$" CNAME
    type_cast_literal: CNAME "`" /[^`]+/ "`"
    ?literal: NUMBER | STRING | CONST_VALUE | type_cast_literal
    func_parma_kv: CNAME "=" literal
    ?func_parma: (literal | func_parma_kv)
    parma_list: "("  [ func_parma  ( ","  func_parma )* ]  ")"
    normal_node: CNAME [ parma_list ]
    apply_concurrency_node: "parallel" [ parma_list ] "{" node  ( ","  node )* "}"
    map_concurrency_node: "map" [ parma_list ] node
    model: "model" CNAME "{" [ link ] "}"
    ref: "&" CNAME
    node: [ CNAME "=" ] (normal_node | model | apply_concurrency_node | map_concurrency_node | choice | redo | while_loop | ref | model_gen_link)
    link: node ("->" node)*
    file_reg_kv: (CNAME [ ":" CNAME ])
    file_reg_kv_list: "{" file_reg_kv ("," file_reg_kv)* "}"
    define_parma_kv: STRING [ "type" CNAME ]
    no_parma: "!"
    context_list: CNAME ("," CNAME)*
    define_source_file: "@aflow" [ file_reg_kv_list ] [ "@in_parma" (define_parma_kv | no_parma | CONST_VALUE) ] [ "@contexts" context_list ]
    choice: "select" CNAME [ parma_list ] "{" model ("," model)* "}"
    while_loop: "do" node "while" node
    redo: "redo" node (NUMBER | CONST_VALUE)
    model_tmpl_param: "("  [ func_parma_kv  ( ","  func_parma_kv )* ]  ")"
    model_gen_link: "%" /[^%]+/ "%" [ model_tmpl_param ]
    """