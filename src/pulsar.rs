// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use pulsar::{Pulsar, TokioExecutor};
use serde::{Serialize, Deserialize};
use serde_env::from_env;
use crate::random_util;

#[derive(Debug, Serialize, Deserialize)]
struct PulsarConfig {
    #[serde(default = "default_localhost")]
    pulsar_host: String,
    #[serde(default = "default_6650")]
    pulsar_port: i32,
    pulsar_topic: String,
    #[serde(default = "default_1024")]
    pulsar_message_size: usize,
}

fn default_localhost() -> String {
    "localhost".to_string()
}

fn default_6650() -> i32 {
    6650
}

fn default_1024() -> usize {
    1024
}

pub async fn start() {
    let pulsar_config: PulsarConfig = from_env().expect("deserialize from env");
    let addr = format!("pulsar://{}:{}", pulsar_config.pulsar_host, pulsar_config.pulsar_port);
    let result = Pulsar::builder(addr, TokioExecutor).build().await;
    match result {
        Ok(pulsar) => {
            let result = pulsar
                .producer()
                .with_topic(pulsar_config.pulsar_topic)
                .build()
                .await;
            match result {
                Ok(mut producer) => {
                    loop {
                        producer
                            .send(random_util::random_str(pulsar_config.pulsar_message_size))
                            .await.unwrap();
                    }
                }
                Err(e) => {
                    println!("create producer failed: {}", e);
                }
            }
        }
        Err(e) => {
            println!("connect failed: {}", e);
        }
    }
}
