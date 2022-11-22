package endpoint

import (
	"fmt"
	gormMysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"testing"
)

func TestTimeConsuming(t *testing.T) {
	dsn := "yangtuojia001:yangtuojia001@tcp(172.16.50.221:3306)/test?charset=utf8&parseTime=True&loc=Local"
	db, _ := gorm.Open(gormMysql.New(gormMysql.Config{
		DSN:                       dsn,   // DSN data source name
		DefaultStringSize:         256,   // string 类型字段的默认长度
		DisableDatetimePrecision:  true,  // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,  // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,  // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false, // 根据当前 MySQL 版本自动配置
	}), &gorm.Config{})

	type Tables struct {
		TableName string
	}

	type TableDesc struct {
		Table       string
		CreateTable string `gorm:"column:Create Table"`
	}

	//sql := `select table_name from information_schema.tables where TABLE_SCHEMA = "yt_otter";`
	//tableList := make([]Tables,10)
	//db.Exec(sql).Scan(&tableList)
	//db.Table("information_schema.tables").Select("table_name").Where("TABLE_SCHEMA = ?", "yt_otter").Scan(&tableList)
	//db.Table("tables").Where("TABLE_SCHEMA = ?","yt_otter").Select("TABLE_NAME")
	//var tableDesc TableDesc
	var err error
	var val []interface{}
	val = append(val, 1)
	val = append(val, "1")
	val = append(val, 1)
	err = db.Exec("insert into test.t_voucher (ID, vono,dc_id) values (?, ?, ?)", val...).Error
	fmt.Println(err)

}

func TestIsContain(t *testing.T) {
	var items = make([]string, 0)
	var item = "sa"
	schema := IsContain(items, item)
	fmt.Println(schema)
}

func TestChangeTable(t *testing.T) {
	sql := "CREATE TABLE `budget_dict` (\n  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',\n  `config_code` varchar(20) NOT NULL COMMENT '数据字典-code',\n  `config_value` varchar(50) NOT NULL COMMENT '数据字典-value',\n  `config_seq` int(11) DEFAULT '1' COMMENT '数据字典-顺序',\n  `type_code` varchar(50) NOT NULL COMMENT '数据字典类型编码',\n  `type_name` varchar(50) NOT NULL COMMENT '数据字典类型名称',\n  `enable` tinyint(1) NOT NULL DEFAULT '1' COMMENT '是否启用(1-启用; 0-停用)',\n  `add_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '数据行创建时间',\n  `modified_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '数据行最后修改时间',\n  PRIMARY KEY (`id`) USING BTREE,\n  KEY `idx_add_time` (`add_time`) USING BTREE,\n  KEY `idx_type_code` (`type_code`) USING BTREE,\n  KEY `idx_config_code` (`config_code`) USING BTREE\n) ENGINE=InnoDB AUTO_INCREMENT=35 DEFAULT CHARSET=utf8 COMMENT='预算字典表'"
	oldTable := "budget_dict"
	newTable := "t2"

	newSQL := createTableSqlChangeTableName(sql, oldTable, newTable)
	fmt.Println(len(newSQL))
	fmt.Println(newSQL)
}
