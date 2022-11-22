package util

import (
	"fmt"
	"testing"
)

func TestDdlChangeTableName(t *testing.T) {
	sql := "ALTER TABLE `site_page`.`t12` \n\tMODIFY COLUMN `tab_logo` varchar(256) NULL COMMENT '页签logo' AFTER `tab_name`;"
	newTable := "t1232"

	newSQL, err := DDLChangeTableName(sql, newTable)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(len(newSQL))
		fmt.Println(newSQL)
	}
}
