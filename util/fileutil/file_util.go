/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */
package fileutil

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 获取程序运行路径
func GetCurrentDirectory() string {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		fmt.Errorf(err.Error())
	}
	return strings.Replace(dir, "\\", "/", -1)
}

// 判断给定的文件路径是否存在
func IsExist(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}

// 判断给定的路径是否是文件夹
func IsDir(path string) bool {
	if stat, err := os.Stat(path); err == nil {
		return stat.IsDir()
	}
	return false
}

// 给定的文件不存在则创建
func CreateFileIfNecessary(path string) bool {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		if file, err := os.Create(path); err == nil {
			file.Close()
		}
	}
	exist := IsExist(path)
	return exist
}

// 给定的目录不存在则创建
func MkdirIfNecessary(path string) error {
	_, err := os.Stat(path)
	if err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(path, os.ModePerm); err != nil {
			// os.Chmod(path, 0777)
			return err
		}
	}
	return nil
}
