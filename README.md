go-modeldb
==========

A simple wrapper around sql.DB to help with structs. Not quite an ORM.
Philosophy: Don't make an ORM

Example:

```golang
// Setup
require "modeldb"
db := ...
modeldb.SetDB(db)


// Declaring a model
type User struct {                                                                                                                                                                                                                                                                       
    Id       string `json:"id"      db:"id,autoinc"`
    Email    string `json:"email"   db:"email,null"`
    Scrypt   []byte `json:"-"       db:"scrypt"`
    Salt     []byte `json:"-"       db:"salt"`
}
var UserModel = db.GetModelInfo(new(User))


// Inserting a model
user := &User{Email:email, Scrypt:scrypt, Salt:salt}

_, err = modeldb.Exec(
    `INSERT INTO user (`+UserModel.FieldsInsert+`)
     VALUES (`+UserModel.Placeholders+`)`,
    user,
)


// Querying a model
var user User                                                                                                                                                                                                                                                                        
err := modeldb.QueryRow(
    `SELECT `+UserModel.FieldsSimple+`
     FROM user WHERE email=?`,
    email,
).Scan(&user)


// Querying many rows
rows, err := modeldb.QueryAll(User{},
    `SELECT `+UserModel.FieldsSimple+`
     FROM user WHERE id < 100`)
)
if err != nil { panic(err) }
users := rows.([]*User)
```

Transactions are also supported!
