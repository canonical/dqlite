// Copyright 2017 Canonical Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package protocol_test

import (
	"fmt"
	"log"

	"github.com/CanonicalLtd/dqlite/internal/protocol"
)

// Create a new dqlite Command with a few parameters, serialize it, and the
// finally deserialize it and read back its parameters.
func Example() {
	data, err := protocol.MarshalCommand(protocol.NewOpen("test.db"))
	if err != nil {
		log.Fatalf("failed to marshal open command: %v", err)
	}
	cmd, err := protocol.UnmarshalCommand(data)
	if err != nil {
		log.Fatalf("failed to unmarshal open command: %v", err)
	}
	params := cmd.Payload.(*protocol.Command_Open)

	// Output:
	// 11
	// open
	// test.db
	fmt.Println(len(data))
	fmt.Println(cmd.Name())
	fmt.Println(params.Open.Name)
}
