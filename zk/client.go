// Copyright (C) 2015  The GoHBase Authors.  All rights reserved.
// This file is part of GoHBase.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the COPYING file.

// Package zk encapsulates our interactions with ZooKeeper.
package zk

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/tsuna/gohbase/logger"

	"github.com/dropbox/gozk/zookeeper"
	"github.com/golang/protobuf/proto"
	"github.com/tsuna/gohbase/pb"
)

// ResourceName is a type alias that is used to represent different resources
// in ZooKeeper
type ResourceName string

// Meta is a ResourceName that indicates that the location of the Meta
// table is what will be fetched
var Meta ResourceName

// Master is a ResourceName that indicates that the location of the Master
// server is what will be fetched
var Master ResourceName

// log is used to standardize logging across all subpackages
var log = logger.Log

const (
	sessionTimeout = 30
	znodeRoot      = "hbase"

	MetaTemplate   = "/%s/meta-region-server"
	MasterTemplate = "/%s/master"
)

func init() {
	SetZnodeRoot(znodeRoot)
}

// SetZnodeRoot sets the Zookeeper parent namespace
func SetZnodeRoot(name string) {
	Meta = ResourceName(fmt.Sprintf(MetaTemplate, name))
	Master = ResourceName(fmt.Sprintf(MasterTemplate, name))
}

// LocateResource returns the location of the specified resource.
func LocateResource(zkquorum string, resource ResourceName) (string, uint16, error) {
	zkconn, _, err := zookeeper.Dial(zkquorum, time.Duration(sessionTimeout)*time.Second)
	if err != nil {
		return "", 0,
			fmt.Errorf("Error connecting to ZooKeeper at %v: %s", zkquorum, err)
	}
	defer zkconn.Close()
	sbuf, _, err := zkconn.Get(string(resource))

	buf := []byte(sbuf)
	if err != nil {
		return "", 0,
			fmt.Errorf("Failed to read the %s znode: %s", resource, err)
	}
	if len(buf) == 0 {
		log.Fatalf("%s was empty!", resource)
	} else if buf[0] != 0xFF {
		return "", 0,
			fmt.Errorf("The first byte of %s was 0x%x, not 0xFF", resource, buf[0])
	}
	metadataLen := binary.BigEndian.Uint32(buf[1:])
	if metadataLen < 1 || metadataLen > 65000 {
		return "", 0, fmt.Errorf("Invalid metadata length for %s: %d", resource, metadataLen)
	}
	buf = buf[1+4+metadataLen:]
	magic := binary.BigEndian.Uint32(buf)
	const pbufMagic = 1346524486 // 4 bytes: "PBUF"

	if magic != pbufMagic {
		return "", 0, fmt.Errorf("Invalid magic number for %s: %d", resource, magic)
	}
	buf = buf[4:]
	var server *pb.ServerName
	if resource == Meta {
		meta := &pb.MetaRegionServer{}
		err = proto.UnmarshalMerge(buf, meta)
		if err != nil {
			return "", 0,
				fmt.Errorf("Failed to deserialize the MetaRegionServer entry from ZK: %s", err)
		}
		server = meta.Server
	} else {
		master := &pb.Master{}
		err = proto.UnmarshalMerge(buf, master)
		if err != nil {
			return "", 0,
				fmt.Errorf("Failed to deserialize the Master entry from ZK: %s", err)
		}
		server = master.Master
	}
	return *server.HostName, uint16(*server.Port), nil
}
