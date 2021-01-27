use std::{ thread::{sleep}, time, process};

use nats::{Connection, Message, Subscription};

use tiberius::{AuthMethod, Client, Config, ExecuteResult, FromSqlOwned};
// use tokio::net::TcpStream;
// use tokio_util::compat::TokioAsyncWriteCompatExt;
// use futures::StreamExt;
use docopt::{ArgvMap, Docopt};
use serde::{Deserialize, Serialize};
use async_std::{
    net::{TcpStream},
};
// static CONN_STR: Lazy<String> = Lazy::new(|| {
//     env::var("CONNECTION_STRING").unwrap_or_else(|_| {
//         "server=tcp:localhost,1433;database=nrm72kulga;IntegratedSecurity=true;TrustServerCertificate=true".to_owned()
//     })
// });

// use tiberius::SqlBrowser;
// use tiberius::{AuthMethod, Client, Config, FromSqlOwned};
// use tokio::net::TcpStream;
// // use async_std::net::TcpStream;
// use tokio_util::compat::Tokio02AsyncWriteCompatExt;
// // use futures::StreamExt;
// use docopt::Docopt;

const USAGE: &'static str = "
nats-resgate-mssql-service.

Usage:
  nats-resgate-mssql-service [--nats_uri=<uri>] [--host=<kn>] [--database=<kn>]  [--instance=<kn>]

Options:
  --nats_uri=<kn>  Speed in knots [default: localhost:4222].
  --host=<h>  SQL host [default: localhost].
  --database=<kn>  SQL database [default: nrm72kulga].
  --instance=<kn>  SQL database.
";

// #[derive(Debug, Deserialize)]
// struct Args {
//     flag_speed: isize,
//     flag_drifting: bool,
//     nats_uri: Vec<String>,
//     arg_x: Option<i32>,
//     arg_y: Option<i32>,
//     cmd_ship: bool,
//     cmd_mine: bool,
// }

#[derive(Serialize, Deserialize)]
struct Model {
    message: String,
}

#[derive(Serialize, Deserialize)]
struct ResultR {
    model: Model,
}

#[derive(Serialize, Deserialize)]
struct CallResponse {
    result: String,
}

#[derive(Serialize, Deserialize)]
struct Response {
    result: ResultR,
}
#[derive(Serialize, Deserialize)]
struct ResultAccess {
    get: bool,
    call: String,
}
#[derive(Serialize, Deserialize)]
struct Access {
    result: ResultAccess,
}
#[derive(Deserialize)]
struct Data {
    params: String,
}

// // main
// fn run() -> Result<()> {
//     let fut = accept_loop("127.0.0.1:8080");
//     task::block_on(fut)
// }
// #[async_std::main]
// async 
fn main() -> std::io::Result<()> {
    // let opts = nats::Options::new();
    let args = Docopt::new(USAGE)
        .and_then(|dopt| dopt.parse())
        .unwrap_or_else(|e| e.exit());
    // const ARGS = args.clone();
    let nats_url: &str = args.get_str("--nats_uri");
    println!("{:?}", args);

    println!("\nSome values:");
    println!(" NATS: {}", nats_url);

    let nc_access = nats::connect(nats_url)?;
    println!("connected to NATS: {}", nats_url);

    nc_access
        .subscribe("access.example.model")?
        .with_handler(move |msg| {
            println!("Received {}", &msg);
            let response = Access {
                result: ResultAccess {
                    get: true,
                    call: "*".to_owned(),
                },
            };

            let j = serde_json::to_vec(&response)?;
            // println!("Reply {}", j);
            msg.respond(j)?;
            Ok(())
        });

    // let config = Config::from_ado_string(&CONN_STR)?;
    let mut config = Config::new();
    println!("  host: {}", args.get_str("--host"));
    config.host(args.get_str("--host"));

    // The default port of SQL Browser
    config.database(args.get_str("--database"));

    config.trust_cert();

    // And from here on continue the connection process in a normal way.
    // let mut client = Client::connect(config, tcp).await?;

    // Using SQL Server authentication.
    config.authentication(AuthMethod::sql_server("bot", "bot"));

    // // Taking the address from the configuration, using async-std's
    // // TcpStream to connect to the server.
    // let tcp = TcpStream::connect(config.get_addr()).await?;

    // let instance: &str = args.get_str("--instance");

    // let tcp: TcpStream;
    // // let mut client: Client<TcpStream>;
    // // println!("  instance: {}", instance);
    // // if instance == "" {
    // // config.port(1433);
    // tcp = TcpStream::connect(config.get_addr()).await?;
    // //   mut client = Client::connect(config, tcp).await?;
    // // } else {
    // //     // The name of the database server instance.
    // //     config.port(1434);
    // //     config.instance_name(instance);
    // //     // This will create a new `TcpStream` from `async-std`, connected to the
    // //     // right port of the named instance.
    // //     tcp = TcpStream::connect_named(&config).await?;
    // // }
    // tcp.set_nodelay(true)?;

    // let sub = nc.subscribe("call.example.model.*")?;
    // // .with_handler(move |msg| {
    // for msg in sub.messages() {
    //     match nc.publish(
    //         msg.reply.clone().unwrap_or_default().as_str(),
    //         "timeout:\"30000\"",
    //     ) {
    //         Ok(_) => {f1(msg, &client).await?},
    //         Err(e) => {
    //             eprintln!("error: {:?}", e);
    //         }
    //     };
    // }

    // let cfg_prt: &'static Config = &config;

    for i in 0..5 {
        std::thread::spawn(move || {
            let args = Docopt::new(USAGE)
                .and_then(|dopt| dopt.parse())
                .unwrap_or_else(|e| e.exit());
            let config = mssql_config(args.to_owned());
            // let tcp: TcpStream;
            // let mut client: Client<TcpStream>;
            // println!("  instance: {}", instance);
            // if instance == "" {
            // config.port(1433);
            match futures::executor::block_on(TcpStream::connect(config.get_addr())) {
                Err(e) => {
                    eprintln!("{}", e);
                    process::exit(1);
                }
                Ok(tcp) => {
                    tcp.set_nodelay(true).unwrap();
                    match futures::executor::block_on(Client::connect(config, tcp)) {
                        Err(e) => {println!("{}", e)}
                        Ok(mut client) => {
                            let nats_url: &str = args.get_str("--nats_uri");
                            match nats::connect(nats_url) {
                                Err(e) => {println!("{}", e)}
                                Ok(nc) => {
                                    println!("{} connected to NATS: {}", i, nats_url);
                                    match nc.queue_subscribe("call.example.model.*", "mssql") {
                                        Ok(sub) => {
                                            for msg in sub.messages() {
                                                println!("Received in {} {}", i, &msg);
                                                match nc.publish(
                                                    msg.reply.clone().unwrap_or_default().as_str(),
                                                    "timeout:\"30000\"",
                                                ) {
                                                    Err(_e) => {}
                                                    Ok(()) => {
                                                        match serde_json::from_slice::<Data>(
                                                            &msg.data[..],
                                                        ) {
                                                            Ok(data) => {
                                                                let split = msg.subject.split(".");
                                                                let v: Vec<&str> = split.collect();
                                                                let method =
                                                                    v.last().unwrap().as_ref();
                                                                match method {
                                                                    "exec" => {
                                                                        match futures::executor::block_on(
                                                                    client
                                                                        .execute(data.params, &[]),
                                                                ) {
                                                                    Ok(result) => {
                                                                        // let result = client.execute(data.params, &[]).await?;

                                                                        let payload = format!(
                                                                            "{}",
                                                                            result.total()
                                                                        );
                                                                        println!("{}", payload);
                                                                        let response =
                                                                            CallResponse {
                                                                                result: payload
                                                                                    .to_owned(),
                                                                            };
                                                                        // println!("response {}", &response);
                                                                        match serde_json::to_vec(
                                                                            &response,
                                                                        ) {
                                                                            Ok(json) => {
                                                                                // println!("Reply to {}", msg.reply.to_owned().unwrap());
                                                                                // println!("Reply {}", j.to_string());
                                                                                match msg
                                                                                    .respond(json)
                                                                                {
                                                                                    Err(e) => {}
                                                                                    Ok(_) => {}
                                                                                }
                                                                            }
                                                                            Err(e) => {}
                                                                        }
                                                                    }
                                                                    Err(e) => {}
                                                                }
                                                                    }
                                                                    // "simple_query" => println!("simple_query"),
                                                                    _ => {
                                                                        match futures::executor::block_on(
                                                                    client
                                                                        .simple_query(data.params),
                                                                ) {
                                                                    Ok(mut stream) => {
                                                                        if stream.next_resultset() {
                                                                            match futures::executor::block_on(
                                                                    stream.into_row(),
                                                                ) {
                                                                    Ok(row) => {
                                                                        match row {
                                                                            Some(r) => {
                                                                                for val in
                                                                                    r.into_iter()
                                                                                {
                                                                                    match String::from_sql_owned(val) {
                                                                        Ok(payload_option) => {
                                                                            match payload_option {
                                                                                Some(payload) => {
                                                                                    println!("{}", payload);
                                                                                    let response =
                                                                                        CallResponse { result: payload };
                                                                                    // println!("response {}", &response);
                                                                                    match serde_json::to_vec(&response) {
                                                                                        Ok(json) => {
                                                                                            // println!("Reply to {}", msg.reply.to_owned().unwrap());
                                                                                            // println!("Reply {}", j.to_string());
                                                                                            match msg.respond(json) {
                                                                                                Err(e) => {},
                                                                                                // Ok(_) => {} 
                                                                                                Ok(_) => {},
                                                                                            }
                                                                                        }
                                                                                        Err(e) => {}
                                                                                    }
                                                                                }
                                                                                None => {
                                                                                    println!("none value");
                                                                                    let response = CallResponse {
                                                                                        result: String::from("none"),
                                                                                    };
                                                                                    // "error": {
                                                                                    //     "code": "system.invalidParams",
                                                                                    //     "message": "Invalid parameters"
                                                                                    // }
                                                                                    // println!("response {}", &response);
                                                                                    match serde_json::to_vec(&response) {
                                                                                        Ok(json) => {
                                                                                            // println!("Reply to {}", msg.reply.to_owned().unwrap());
                                                                                            // println!("Reply {}", j.to_string());
                                                                                            match msg.respond(json) {
                                                                                                Err(_) => {},
                                                                                                // Ok(_) => {} 
                                                                                                Ok(_) => {},
                                                                                            }
                                                                                        }
                                                                                        Err(_) => {}
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                        Err(e) => {},
                                                                    }
                                                                                }
                                                                            }
                                                                            None => {
                                                                                println!(
                                                                                    "none row"
                                                                                );
                                                                                let response =
                                                                            CallResponse {
                                                                                result:
                                                                                    String::from(
                                                                                        "none",
                                                                                    ),
                                                                            };
                                                                                match serde_json::to_vec(
                                                                            &response,
                                                                        ) {
                                                                            Ok(json) => {
                                                                                // println!("Reply to {}", msg.reply.to_owned().unwrap());
                                                                                // println!("Reply {}", j.to_string());
                                                                                match msg
                                                                                    .respond(json)
                                                                                {
                                                                                    Err(_) => {}
                                                                                    Ok(_) => {} // Ok(v) => println!("sended: {:?}", v),
                                                                                }
                                                                            }
                                                                            Err(_) => {}
                                                                        }
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(_) => {}
                                                                }
                                                                        }
                                                                    }
                                                                    Err(_) => {}
                                                                }
                                                                    }
                                                                }
                                                            }
                                                            Err(_) => {}
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(_) => {}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }
    loop { sleep(time::Duration::new(9,0))}
}

fn mssql_config(args: ArgvMap) -> Config {
    let mut config = Config::new();
    println!("  host: {}", args.get_str("--host"));
    config.host(args.get_str("--host"));

    // The default port of SQL Browser
    config.database(args.get_str("--database"));

    config.trust_cert();

    // And from here on continue the connection process in a normal way.
    // let mut client = Client::connect(config, tcp).await?;

    // Using SQL Server authentication.
    config.authentication(AuthMethod::sql_server("bot", "bot"));
    config
}
// async fn f1(
//     msg: Message,
//     client: &Client<tokio_util::compat::Compat<TcpStream>>,
// // ) -> std::result::Result<(), ()> {
// ) -> anyhow::Result<()> {
//     println!("Received a {}", msg);
//     let data: Data = serde_json::from_slice(&msg.data[..])?;
//     let split = msg.subject.split(".");
//     let v: Vec<&str> = split.collect();
//     let method = v.last().unwrap().as_ref();
//     match method {
//         "exec" => {
//             let result = client.execute(data.params, &[]).await?;

//             // As long as the `next_resultset` returns true, the stream has
//             // more results and can be polled. For each result set, the stream
//             // returns rows until the end of that result. In a case where
//             // `next_resultset` is true, polling again will return rows from
//             // the next query.
//             // result.rows_affected()
//             // assert!(stream.next_resultset());
//             // In this case, we know we have only one query, returning one row
//             // and one column, so calling `into_row` will consume the stream
//             // and return us the first row of the first result.
//             let payload = format!("{}", result.total());
//             println!("{}", payload);
//             let response = CallResponse {
//                 result: payload.to_owned(),
//             };
//             // println!("response {}", &response);
//             let j = serde_json::to_vec(&response)?;
//             // println!("Reply to {}", msg.reply.to_owned().unwrap());
//             // println!("Reply {}", j.to_string());
//             match msg.respond(j) {
//                 Err(e) => eprintln!("error: {:?}", e),
//                 Ok(_) => {} // Ok(v) => println!("sended: {:?}", v),
//             }
//         }
//         "simple_query" => println!("simple_query"),
//         // Handle the rest of cases
//         _ => {
//             let mut stream = client.simple_query(data.params).await?;

//             // As long as the `next_resultset` returns true, the stream has
//             // more results and can be polled. For each result set, the stream
//             // returns rows until the end of that result. In a case where
//             // `next_resultset` is true, polling again will return rows from
//             // the next query.
//             // assert!(stream.next_resultset());
//             if stream.next_resultset() {
//                 // In this case, we know we have only one query, returning one row
//                 // and one column, so calling `into_row` will consume the stream
//                 // and return us the first row of the first result.
//                 let row = stream.into_row().await?;
//                 match row {
//                     Some(r) => {
//                         // let payload: &str = r.get(0).unwrap();
//                         for val in r.into_iter() {
//                             // assert_eq!(
//                             //     Some(String::from("test")),
//                             //     String::from_sql_owned(val)?
//                             // )
//                             // let pp = r.get(0);
//                             // let ss = String::from_sql_owned(pp);
//                             match String::from_sql_owned(val) {
//                                 Ok(payload_option) => {
//                                     // let payload: &str = payloadv.as_str();
//                                     match payload_option {
//                                         Some(payload) => {
//                                             println!("{}", payload);

//                                             let response = CallResponse { result: payload };

//                                             // println!("response {}", &response);
//                                             let j = serde_json::to_vec(&response)?;
//                                             // println!("Reply to {}", msg.reply.to_owned().unwrap());
//                                             // println!("Reply {}", j.to_string());
//                                             match msg.respond(j) {
//                                                 Ok(v) => println!("sended: {:?}", v),
//                                                 Err(e) => println!("error: {:?}", e),
//                                             }
//                                         }
//                                         None => {
//                                             println!("none value");
//                                             let response = CallResponse {
//                                                 result: String::from("none"),
//                                             };
//                                             // "error": {
//                                             //     "code": "system.invalidParams",
//                                             //     "message": "Invalid parameters"
//                                             // }
//                                             // println!("response {}", &response);
//                                             let j = serde_json::to_vec(&response)?;
//                                             // println!("Reply to {}", msg.reply.to_owned().unwrap());
//                                             // println!("Reply {}", j.to_string());
//                                             match msg.respond(j) {
//                                                 Ok(v) => println!("sended: {:?}", v),
//                                                 Err(e) => println!("error: {:?}", e),
//                                             }
//                                         }
//                                     }
//                                 }
//                                 Err(e) => println!("error: {:?}", e),
//                             }
//                         }
//                     }
//                     None => println!("none row"),
//                 }
//             }
//         }
//     }
//     Ok(())
// }
