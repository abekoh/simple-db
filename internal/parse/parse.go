package parse

type Lexer struct {
	s            string
	cursor       int
	currentToken string
}

type token string

const (
	selectTok token = "select"
	from      token = "from"
	and       token = "and"
	where     token = "where"
	insert    token = "insert"
	into      token = "into"
	values    token = "values"
	deleteTok token = "delete"
	update    token = "update"
	set       token = "set"
	create    token = "create"
	table     token = "table"
	intTok    token = "int"
	varchar   token = "varchar"
	view      token = "view"
	as        token = "as"
	index     token = "index"
	on        token = "on"
)
