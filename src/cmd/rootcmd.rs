use crate::cmd::gen_file_cmd::new_gen_file_cmd;
use crate::cmd::osstaskcmd::new_osstask_cmd;
use crate::cmd::{new_config_cmd, new_osscfg_cmd};
use crate::commons::yamlutile::struct_to_yml_file;
use crate::commons::{generate_file, read_yaml_file, SubCmd};
use crate::commons::{struct_to_yaml_string, CommandCompleter};
use crate::configure::{generate_default_config, set_config_file_path};
use crate::configure::{get_config_file_path, get_current_config_yml, set_config};
use crate::interact;
use crate::osstask::{
    task_id_generator, Task, TaskDefaultParameters, TaskDescription, TaskDownload,
    TaskLocalToLocal, TaskTransfer, TaskUpLoad,
};
use crate::s3::oss::OSSDescription;
use crate::s3::oss::OssProvider;
use clap::{Arg, ArgMatches};
use clap::{ArgAction, Command as Clap_Command};
use lazy_static::lazy_static;
use std::borrow::Borrow;

pub const APP_NAME: &'static str = "oss_pipe";
lazy_static! {
    static ref CLIAPP: Clap_Command = Clap_Command::new(APP_NAME)
        .version("1.0")
        .author("Shiwen Jia. <jiashiwen@gmail.com>")
        .about("RustBoot")
        .arg_required_else_help(true)
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("Sets a custom config file")
        )
        .arg(
            Arg::new("interact")
                .short('i')
                .long("interact")
                .action(ArgAction::SetTrue)
                .help("run as interact mod")
        )
        .subcommand(new_osstask_cmd())
        .subcommand(new_config_cmd())
        .subcommand(new_osscfg_cmd())
        .subcommand(new_gen_file_cmd());
    static ref SUBCMDS: Vec<SubCmd> = subcommands();
}

pub fn run_app() {
    set_config("");
    let matches = CLIAPP.clone().get_matches();
    cmd_match(&matches);
}

pub fn run_from(args: Vec<String>) {
    match Clap_Command::try_get_matches_from(CLIAPP.to_owned(), args.clone()) {
        Ok(matches) => {
            cmd_match(&matches);
        }
        Err(err) => {
            err.print().expect("Error writing Error");
        }
    };
}

// 获取全部子命令，用于构建commandcompleter
pub fn all_subcommand(app: &Clap_Command, beginlevel: usize, input: &mut Vec<SubCmd>) {
    let nextlevel = beginlevel + 1;
    let mut subcmds = vec![];
    for iterm in app.get_subcommands() {
        subcmds.push(iterm.get_name().to_string());
        if iterm.has_subcommands() {
            all_subcommand(iterm, nextlevel, input);
        } else {
            if beginlevel == 0 {
                all_subcommand(iterm, nextlevel, input);
            }
        }
    }
    let subcommand = SubCmd {
        level: beginlevel,
        command_name: app.get_name().to_string(),
        subcommands: subcmds,
    };
    input.push(subcommand);
}

pub fn get_command_completer() -> CommandCompleter {
    CommandCompleter::new(SUBCMDS.to_vec())
}

fn subcommands() -> Vec<SubCmd> {
    let mut subcmds = vec![];
    all_subcommand(CLIAPP.clone().borrow(), 0, &mut subcmds);
    subcmds
}

fn cmd_match(matches: &ArgMatches) {
    if let Some(c) = matches.get_one::<String>("config") {
        set_config_file_path(c.to_string());
        set_config(&get_config_file_path());
    } else {
        set_config("");
    }

    if matches.get_flag("interact") {
        interact::run();
        return;
    }

    // if let Some(ref matches) = matches.subcommand_matches("start") {
    //     if matches.get_flag("daemon") {
    //         let args: Vec<String> = env::args().collect();
    //         if let Ok(Fork::Child) = daemon(true, true) {
    //             // Start child thread
    //             let mut cmd = Command::new(&args[0]);
    //             for idx in 1..args.len() {
    //                 let arg = args.get(idx).expect("get cmd arg error!");
    //                 // remove start as daemon variable
    //                 // 去除后台启动参数,避免重复启动
    //                 if arg.eq("-d") || arg.eq("-daemon") {
    //                     continue;
    //                 }
    //                 cmd.arg(arg);
    //             }

    //             let child = cmd.spawn().expect("Child process failed to start.");
    //             fs::write("pid", child.id().to_string()).expect("Write pid file error!");
    //         }
    //         println!("{}", "daemon mod");
    //         std::process::exit(0);
    //     }

    //     println!("current pid is:{}", std::process::id());

    //     //校验本地任务目录是否存在，没有就创建
    //     check_local_desc_path().unwrap();

    //     // check config
    //     let cfg = get_config().unwrap();
    //     let task_ping_handle = thread::spawn(move || {
    //         let c = cfg.clone();

    //         let rt = runtime::Builder::new_multi_thread()
    //             .worker_threads(c.threads)
    //             .enable_io()
    //             .enable_time()
    //             .build()
    //             .unwrap();

    //         rt.block_on(async {});
    //     });
    //     let cfg = get_config().unwrap();
    //     let task_curl_handle = thread::spawn(move || {
    //         let c = cfg.clone();
    //         let rt = runtime::Builder::new_multi_thread()
    //             .worker_threads(c.threads)
    //             .enable_io()
    //             .enable_time()
    //             .build()
    //             .unwrap();
    //         rt.block_on(async {});
    //     });
    //     task_ping_handle.join().unwrap();
    //     task_curl_handle.join().unwrap();
    // }

    // if let Some(ref _matches) = matches.subcommand_matches("stop") {
    //     println!("server stopping...");
    //     // let sys = System::new_with_specifics(RefreshKind::with_processes(Default::default()));
    //     let r = RefreshKind::new();
    //     let r = r.with_processes(ProcessRefreshKind::everything());
    //     let sys = System::new_with_specifics(r);

    //     let pidstr = String::from_utf8(fs::read("pid").unwrap()).unwrap();
    //     let pid = Pid::from_str(pidstr.as_str()).unwrap();

    //     if let Some(p) = sys.process(pid) {
    //         println!("terminal process: {:?}", p.pid());
    //     } else {
    //         println!("Server not run!");
    //         return;
    //     };
    //     Command::new("kill")
    //         .args(["-15", pidstr.as_str()])
    //         .output()
    //         .expect("failed to execute process");
    // }

    if let Some(config) = matches.subcommand_matches("config") {
        if let Some(_show) = config.subcommand_matches("show") {
            let yml = get_current_config_yml();
            match yml {
                Ok(str) => {
                    println!("{}", str);
                }
                Err(e) => {
                    eprintln!("{}", e);
                }
            }
        }

        if let Some(gen_config) = config.subcommand_matches("gendefault") {
            let mut file = String::from("");
            if let Some(path) = gen_config.get_one::<String>("filepath") {
                file.push_str(path);
            } else {
                file.push_str("config_default.yml")
            }
            if let Err(e) = generate_default_config(file.as_str()) {
                log::error!("{}", e);
                return;
            };
            println!("{} created!", file);
        }
    }

    if let Some(osstask) = matches.subcommand_matches("osstask") {
        if let Some(f) = osstask.get_one::<String>("filepath") {
            let upload = read_yaml_file::<Task>(f);
            let now = time::Instant::now();

            match upload {
                Ok(t) => {
                    log::info!("execute task: {:?}", t.task_id);
                    let r = t.task_desc.exec_multi_threads();
                    match r {
                        Ok(_) => log::info!("task {} execute ok!", t.task_id),
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    }
                }
                Err(e) => {
                    log::error!("{}", e);
                }
            }
            println!("{:?}", now.elapsed());

            // let rt = tokio::runtime::Runtime::new().unwrap();
            // let async_req = async {
            //     match upload {
            //         Ok(t) => {
            //             log::info!("execute task: {:?}", t.task_id);
            //             let r = t.task_desc.exec().await;
            //             match r {
            //                 Ok(_) => log::info!("task {} execute ok!", t.task_id),
            //                 Err(e) => {
            //                     log::error!("{}", e);
            //                 }
            //             }
            //         }
            //         Err(e) => {
            //             log::error!("{}", e);
            //         }
            //     }
            // };
            // rt.block_on(async_req);
        }

        if let Some(transfer) = osstask.subcommand_matches("transfer") {
            if let Some(f) = transfer.get_one::<String>("filepath") {
                let task = match read_yaml_file::<Task>(f) {
                    Ok(t) => t,
                    Err(e) => {
                        log::error!("{:?}", e);
                        return;
                    }
                };
                let r = task.task_desc.exec_multi_threads();
                match r {
                    Ok(_) => {
                        log::info!("transfer task {} execute ok!", task.task_id)
                    }
                    Err(e) => {
                        log::error!("{}", e);
                    }
                }
            }
        }

        if let Some(download) = osstask.subcommand_matches("download") {
            if let Some(f) = download.get_one::<String>("filepath") {
                let task = read_yaml_file::<Task>(f);

                match task {
                    Ok(t) => {
                        log::info!("execute download task {:?} begin", t.task_id.clone());
                        let r = t.task_desc.exec_multi_threads();
                        match r {
                            Ok(_) => log::info!("download task {} execute ok!", t.task_id),
                            Err(e) => {
                                log::error!("{}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("{}", e);
                    }
                }
            }
        }

        if let Some(download) = osstask.subcommand_matches("download_multithread") {
            if let Some(f) = download.get_one::<String>("filepath") {
                let download = read_yaml_file::<TaskDownload>(f);
                match download {
                    Ok(d) => {
                        let r = d.execute_multi_thread();
                        println!("{:?}", r);
                    }
                    Err(e) => {
                        log::error!("{}", e);
                    }
                }
            }
        }

        if let Some(download) = osstask.subcommand_matches("download_tokio") {
            if let Some(f) = download.get_one::<String>("filepath") {
                let download = read_yaml_file::<TaskDownload>(f);
                match download {
                    Ok(d) => {
                        let r = d.execute_tokio();
                        println!("{:?}", r);
                    }
                    Err(e) => {
                        log::error!("{}", e);
                    }
                }
            }
        }

        if let Some(upload) = osstask.subcommand_matches("upload") {
            if let Some(f) = upload.get_one::<String>("filepath") {
                let upload = read_yaml_file::<Task>(f);
                match upload {
                    Ok(t) => {
                        log::info!("execute upload task: {:?}", t.task_id);
                        let r = t.task_desc.exec_multi_threads();
                        match r {
                            Ok(_) => log::info!("upload task {} execute ok!", t.task_id),
                            Err(e) => {
                                log::error!("{}", e);
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("{}", e);
                    }
                }
            }
        }

        if let Some(template) = osstask.subcommand_matches("template") {
            let task_id = task_id_generator();
            if let Some(_) = template.subcommand_matches("download") {
                let task_download = TaskDownload::default();
                let task = Task {
                    task_id: task_id.to_string(),
                    name: "download task".to_string(),
                    task_desc: TaskDescription::Download(task_download),
                };
                let yml = struct_to_yaml_string(&task);
                match yml {
                    Ok(str) => println!("{}", str),
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }

            if let Some(_) = template.subcommand_matches("transfer") {
                let task_id = task_id_generator();
                let mut task_transfer = TaskTransfer::default();
                task_transfer.source.provider = OssProvider::Ali;
                task_transfer.source.endpoint = "http://oss-cn-beijing.aliyuncs.com".to_string();
                let task = Task {
                    task_id: task_id.to_string(),
                    name: "transfer task".to_string(),
                    task_desc: TaskDescription::Transfer(task_transfer),
                };
                let yml = struct_to_yaml_string(&task);
                match yml {
                    Ok(str) => println!("{}", str),
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }

            if let Some(_) = template.subcommand_matches("upload") {
                let task_upload = TaskUpLoad::default();
                let task = Task {
                    task_id: task_id.to_string(),
                    name: "upload task".to_string(),
                    task_desc: TaskDescription::Upload(task_upload),
                };
                let yml = struct_to_yaml_string(&task);
                match yml {
                    Ok(str) => println!("{}", str),
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }

            if let Some(_) = template.subcommand_matches("localtolocal") {
                let task_localtolocal = TaskLocalToLocal::default();
                let task = Task {
                    task_id: task_id.to_string(),
                    name: "local to local task".to_string(),
                    task_desc: TaskDescription::LocalToLocal(task_localtolocal),
                };
                let yml = struct_to_yaml_string(&task);
                match yml {
                    Ok(str) => println!("{}", str),
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }
        }
    }

    if let Some(osscfg) = matches.subcommand_matches("osscfg") {
        if let Some(gen) = osscfg.subcommand_matches("gendefault") {
            println!("gen osscfg file");
            let mut file = String::from("");
            if let Some(path) = gen.get_one::<String>("filepath") {
                file.push_str(path);
            } else {
                file.push_str("osscfg_default.yml")
            }
            let oss_jd = OSSDescription::default();
            let mut oss_ali = OSSDescription::default();
            oss_ali.provider = OssProvider::Ali;
            oss_ali.endpoint = "oss-cn-beijing.aliyuncs.com".to_string();
            let vec_oss = vec![oss_jd, oss_ali];
            if let Err(e) = struct_to_yml_file(&vec_oss, file.as_str()) {
                log::error!("{}", e);
                return;
            };
            println!("{} created!", file);
        }
    }

    if let Some(gen_file) = matches.subcommand_matches("gen_file") {
        let file_size: usize = match gen_file.get_one("file_size") {
            Some(s) => *s,
            None => {
                return;
            }
        };
        let batch: usize = match gen_file.get_one("batch") {
            Some(s) => *s,
            None => {
                return;
            }
        };

        let file = match gen_file.get_one::<String>("file_name") {
            Some(s) => s,
            None => {
                return;
            }
        };

        if let Err(e) = generate_file(file_size, batch, file) {
            log::error!("{}", e);
        };
    }
}
