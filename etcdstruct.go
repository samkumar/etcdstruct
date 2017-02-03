/*
 * Copyright (c) 2017 Sam Kumar <samkumar@berkeley.edu>
 * Copyright (c) 2017 University of California, Berkeley
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNERS OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

// Package etcdstruct implements tools to retrieve and store structured
// configuration data in etcd.
package etcdstruct

import (
	"context"

	"github.com/ugorji/go/codec"

	etcd "github.com/coreos/etcd/clientv3"
)

var mp codec.Handle = &codec.MsgpackHandle{}

// EtcdStruct abstracts a structured configuration stored in a single
// key-value pair in etcd. It is encoded using the MsgPack standard.
type EtcdStruct interface {
	SetRetrievedRevision(rev int64)
	GetRetrievedRevision() int64
}

func encodeToken(tok EtcdStruct) ([]byte, error) {
	var encoded []byte

	encoder := codec.NewEncoderBytes(&encoded, mp)
	err := encoder.Encode(tok)
	if err != nil {
		return nil, err
	}

	return encoded, nil
}

func decodeToken(encoded []byte, into interface{}) error {
	decoder := codec.NewDecoderBytes(encoded, mp)
	return decoder.Decode(into)
}

func RetrieveEtcdStruct(ctx context.Context, etcdClient *etcd.Client, etcdKey string, retrieveInto EtcdStruct) error {
	resp, err := etcdClient.Get(ctx, etcdKey)
	if err != nil {
		return err
	}

	/* No token with that name exists. */
	if len(resp.Kvs) == 0 {
		return nil
	}

	err = decodeToken(resp.Kvs[0].Value, retrieveInto)
	if err != nil {
		return err
	}

	retrieveInto.SetRetrievedRevision(resp.Kvs[0].ModRevision)
	return nil
}

func RetrieveEtcdStructs(ctx context.Context, etcdClient *etcd.Client, tofill func(key []byte) EtcdStruct, seterror func(es EtcdStruct, key []byte), etcdKey string, opts ...etcd.OpOption) error {
	resp, err := etcdClient.Get(ctx, etcdKey, opts...)
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		es := tofill(kv.Key)
		err := decodeToken(kv.Value, es)
		if err != nil {
			seterror(es, kv.Key)
		}
		es.SetRetrievedRevision(kv.ModRevision)
	}

	return nil
}

func UpsertEtcdStruct(ctx context.Context, etcdClient *etcd.Client, etcdKey string, tok EtcdStruct) error {
	encoded, err := encodeToken(tok)
	if err != nil {
		return err
	}

	_, err = etcdClient.Put(ctx, etcdKey, string(encoded))
	return err
}

func UpsertEtcdStructAtomic(ctx context.Context, etcdClient *etcd.Client, etcdKey string, tok EtcdStruct) (bool, error) {
	encoded, err := encodeToken(tok)
	if err != nil {
		return false, err
	}

	resp, err := etcdClient.Txn(ctx).
		If(etcd.Compare(etcd.ModRevision(etcdKey), "=", tok.GetRetrievedRevision())).
		Then(etcd.OpPut(etcdKey, string(encoded))).
		Commit()
	if resp != nil {
		return resp.Succeeded, err
	} else {
		return false, err
	}
}

func DeleteEtcdStructs(ctx context.Context, etcdClient *etcd.Client, etcdKey string, opts ...etcd.OpOption) (int64, error) {
	resp, err := etcdClient.Delete(ctx, etcdKey, opts...)
	if resp != nil {
		return resp.Deleted, err
	} else {
		return 0, err
	}
}
