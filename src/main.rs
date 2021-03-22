use std::{process, thread::sleep, time};

use nats::Message;

use tiberius::{AuthMethod, Client, Config, FromSqlOwned};
// use tokio::net::TcpStream;
// use tokio_util::compat::TokioAsyncWriteCompatExt;
// use futures::StreamExt;
use async_std::net::TcpStream;
use docopt::{ArgvMap, Docopt};
use serde::{Deserialize, Serialize};
// use url::form_urlencoded::parse;
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

#[derive(Serialize)]
struct CallResponse<'a> {
    result: &'a str,
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
struct DataParams {
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
                        Err(e) => {
                            println!("{}", e)
                        }
                        Ok(mut client) => {
                            let nats_url: &str = args.get_str("--nats_uri");
                            match nats::connect(nats_url) {
                                Err(e) => {
                                    println!("{}", e)
                                }
                                Ok(nc) => {
                                    println!("{} connected to NATS: {}", i, nats_url);
                                    match nc.queue_subscribe("call.example.model.*", "mssql") {
                                        Ok(sub) => {
                                            for msg in sub.messages() {
                                                println!("Received in {} {}", i, &msg);
                                                match msg.respond("timeout:\"30000\"") {
                                                    Err(e) => {
                                                        eprintln!("{}", e);
                                                        process::exit(1);
                                                    }
                                                    Ok(()) => {
                                                        match serde_json::from_slice::<DataParams>(
                                                            &msg.data[..],
                                                        ) {
                                                            Ok(data) => {
                                                                let method = msg
                                                                    .subject
                                                                    .split(".")
                                                                    .last()
                                                                    .unwrap();
                                                                exec_sql_method(
                                                                    method,
                                                                    &mut client,
                                                                    &data.params,
                                                                    &msg,
                                                                );
                                                            }
                                                            Err(_) => {}
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("{}", e);
                                            process::exit(1);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }
    loop {
        sleep(time::Duration::new(9, 0))
    }
}

fn exec_sql_method(method: &str, client: &mut Client<TcpStream>, sql_text: &str, msg: &Message) {
    println!("{}", sql_text);
    // let sql_decoded: String = parse(sql_text.as_bytes())
    //     .map(|(key, val)| [key, val].concat())
    //     .collect();
    // println!("{}", sql_decoded);
    let r: Result<(), tiberius::error::Error> = match method {
        "exec" => match futures::executor::block_on(client.execute(sql_text, &[])) {
            Ok(result) => {
                let count = result.total().to_string();
                call_msg_respond(msg, count.as_str());
                Ok(())
            }
            Err(e) => Err(e),
        },
        // "simple_query" => println!("simple_query"),
        _ => match futures::executor::block_on(client.simple_query(sql_text)) {
            Ok(mut stream) => {
                if stream.next_resultset() {
                    match futures::executor::block_on(stream.into_row()) {
                        Ok(row) => match row {
                            Some(r) => {
                                for val in r.into_iter() {
                                    match String::from_sql_owned(val) {
                                        Ok(payload_option) => match payload_option {
                                            Some(payload) => {
                                                call_msg_respond(msg, payload.as_str());
                                            }
                                            None => {
                                                println!("none value");
                                                call_msg_respond(msg, "none");
                                            }
                                        },
                                        Err(e) => {
                                            eprintln!("{}", e);
                                            process::exit(1);
                                        }
                                    }
                                }
                                Ok(())
                            }
                            None => {
                                call_msg_respond(msg, "none");
                                Ok(())
                            }
                        },
                        Err(e) => {
                            eprintln!("{}", e);
                            process::exit(1);
                        }
                    }
                } else {
                    eprintln!("{}", false);
                    process::exit(1);
                }
            }
            Err(e) => Err(e),
        },
    };
    match r {
        Ok(_) => {}
        Err(e) => {
            eprintln!("{}", e);
            call_msg_respond(msg, format!("{}", e).as_str());
            match e {
                tiberius::error::Error::Io {
                    kind: _,
                    message: _,
                } => {
                    // eprintln!("{}", message);
                    sleep(time::Duration::new(9, 0));
                    process::exit(1);
                }
                // tiberius::error::Error::Protocol(_) => {}
                // tiberius::error::Error::Encoding(_) => {}
                // tiberius::error::Error::Conversion(_) => {}
                // tiberius::error::Error::Utf8 => {}
                // tiberius::error::Error::Utf16 => {}
                // tiberius::error::Error::ParseInt(_) => {}
                // tiberius::error::Error::Server(te) => {}
                // tiberius::error::Error::Tls(_) => {}
                // tiberius::error::Error::Routing { host, port } => {}
                _ => {}
            }
        }
    }
}

fn call_msg_respond(msg: &Message, result: &str) {
    println!("{}", result);
    let response = CallResponse { result };
    match serde_json::to_vec(&response) {
        Ok(json) => {
            // println!("Reply to {}", msg.reply.to_owned().unwrap());
            // println!("Reply {}", j.to_string());
            msg_respond(&msg, json);
        }
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
    }
}

fn msg_respond(msg: &Message, json: Vec<u8>) {
    match msg.respond(&json) {
        Err(e) => {
            eprintln!("{}", e);
            process::exit(1);
        }
        Ok(_) => {
            // println!("sended: {:?}", &json);
        } // Ok(v) => println!("sended: {:?}", v),
    }
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
