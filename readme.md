# dao

```go

import "github.com/afocus/dao"


func main(){
    c, _ := dao.Create("root:123456@(127.0.0.1:3306)/test?charset=utf8")
    s:=c.NewSession(context.Background())




    // insert 插入


    var u User // or map[string]string
    s.Table("user").Insert(&u)


    // 实现mysql insert on duplicate key update
    // 当存在时更新Cols指定的字段
    s.Table("user").Cols("name","age").Insert(&u)



    // 查询
    s.Table("user").Get(&u)
    // select * from user limit 1
    s.Table("user").Select("name","age").Where("id > ?",100).Get(&u)
    // select name, arge from user where (id > 100) limit 1

    var ulist []User
    s.Table("user").Where("age>?",20).And("sex = 1").OrderBy("age desc").Limit(10).Find(&ulist)
    // select * from user where (age > 20) and (sex = 1) order by age desc limit 1o

    s.Table("user").Limit(10,20).Find(&ulist)
    // select * from user limit 10,20

    s.Query("select * from user where id > ?", 100).Find(&ulist)

    s.Table("user").Count()
    // select count(1) from user




    //













}


```
