use crate::cmd::gen_file_cmd::{new_gen_file_cmd, new_gen_files_cmd};
use crate::cmd::osstaskcmd::new_osstask_cmd;
use crate::cmd::{new_config_cmd, new_exit_cmd, new_osscfg_cmd, new_parameters_cmd, new_template};
use crate::commons::yamlutile::struct_to_yml_file;
use crate::commons::{byte_size_to_usize, generate_file, generate_files, read_yaml_file, SubCmd};
use crate::commons::{struct_to_yaml_string, CommandCompleter};
use crate::configure::{generate_default_config, set_config_file_path};
use crate::configure::{get_config_file_path, get_current_config_yml, set_config};
use crate::interact;
use crate::osstask::{
    task_id_generator, Task, TaskDescription, TaskDownload, TaskLocalToLocal, TaskOssCompare,
    TaskTransfer, TaskTruncateBucket, TaskType, TaskUpLoad,
};
use crate::s3::oss::OSSDescription;
use crate::s3::oss::OssProvider;
use clap::{Arg, ArgMatches};
use clap::{ArgAction, Command as Clap_Command};
use lazy_static::lazy_static;

use crate::interact::INTERACT_STATUS;
use std::borrow::Borrow;

pub const APP_NAME: &'static str = "oss_pipe";
// pub const INTERACT_STATUS: &'static bool = &false;

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
        .subcommand(new_template())
        .subcommand(new_parameters_cmd())
        .subcommand(new_config_cmd())
        .subcommand(new_osscfg_cmd())
        .subcommand(new_gen_file_cmd())
        .subcommand(new_gen_files_cmd())
        .subcommand(new_exit_cmd());
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
        if !INTERACT_STATUS.load(std::sync::atomic::Ordering::SeqCst) {
            interact::run();
            return;
        }
    }

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
            let task = read_yaml_file::<Task>(f);
            let now = time::Instant::now();

            match task {
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
        }
    }
    if let Some(template) = matches.subcommand_matches("template") {
        let task_id = task_id_generator();
        if let Some(download) = template.subcommand_matches("download") {
            let file = download.get_one::<String>("file");
            let mut task_download = TaskDownload::default();
            let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
            let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
            task_download.exclude = Some(exclude_vec);
            task_download.include = Some(include_vec);

            let task = Task {
                task_id: task_id.to_string(),
                name: "download task".to_string(),
                task_desc: TaskDescription::Download(task_download),
            };
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{}", e),
                    }
                }
            };
        }

        if let Some(transfer) = template.subcommand_matches("transfer") {
            let file = transfer.get_one::<String>("file");
            let task_id = task_id_generator();
            let mut task_transfer = TaskTransfer::default();
            let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
            let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
            task_transfer.exclude = Some(exclude_vec);
            task_transfer.include = Some(include_vec);
            task_transfer.source.provider = OssProvider::ALI;
            task_transfer.source.endpoint = "http://oss-cn-beijing.aliyuncs.com".to_string();
            let task = Task {
                task_id: task_id.to_string(),
                name: "transfer task".to_string(),
                task_desc: TaskDescription::Transfer(task_transfer),
            };
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{}", e),
                    }
                }
            };
        }

        if let Some(upload) = template.subcommand_matches("upload") {
            let file = upload.get_one::<String>("file");
            let mut task_upload = TaskUpLoad::default();
            let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
            let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
            task_upload.include = Some(include_vec);
            task_upload.exclude = Some(exclude_vec);
            let task = Task {
                task_id: task_id.to_string(),
                name: "upload task".to_string(),
                task_desc: TaskDescription::Upload(task_upload),
            };
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{}", e),
                    }
                }
            };
        }

        if let Some(localtolocal) = template.subcommand_matches("localtolocal") {
            let file = localtolocal.get_one::<String>("file");

            let task_localtolocal = TaskLocalToLocal::default();
            // let include_vec = vec!["test/t1/*".to_string(), "test/t2/*".to_string()];
            // let exclude_vec = vec!["test/t3/*".to_string(), "test/t4/*".to_string()];
            // task_localtolocal.exclude = Some(exclude_vec);
            // task_localtolocal.include = Some(include_vec);
            let task = Task {
                task_id: task_id.to_string(),
                name: "local to local task".to_string(),
                task_desc: TaskDescription::LocalToLocal(task_localtolocal),
            };
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{}", e),
                    }
                }
            };
        }

        if let Some(truncate_bucket) = template.subcommand_matches("truncate_bucket") {
            let file = truncate_bucket.get_one::<String>("file");
            let task_truncate_bucket = TaskTruncateBucket::default();
            let task = Task {
                task_id: task_id.to_string(),
                name: "truncate bucket task".to_string(),
                task_desc: TaskDescription::TruncateBucket(task_truncate_bucket),
            };
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{}", e),
                    }
                }
            };
        }

        if let Some(oss_compare) = template.subcommand_matches("oss_compare") {
            let file = oss_compare.get_one::<String>("file");
            let mut task_oss_compare = TaskOssCompare::default();
            task_oss_compare.source.provider = OssProvider::ALI;
            task_oss_compare.source.endpoint = "http://oss-cn-beijing.aliyuncs.com".to_string();
            let task = Task {
                task_id: task_id.to_string(),
                name: "oss compare task".to_string(),
                task_desc: TaskDescription::OssCompare(task_oss_compare),
            };
            match file {
                Some(f) => {
                    match struct_to_yml_file(&task, f) {
                        Ok(_) => {
                            println!("Generate {} succeed", f)
                        }
                        Err(e) => {
                            log::error!("{}", e);
                        }
                    };
                }
                None => {
                    let yml = struct_to_yaml_string(&task);
                    match yml {
                        Ok(str) => println!("{}", str),
                        Err(e) => log::error!("{}", e),
                    }
                }
            };
        }
    }

    if let Some(parameters) = matches.subcommand_matches("parameters") {
        if let Some(_) = parameters.subcommand_matches("provider") {
            println!("{:?}", OssProvider::AWS);
            println!("{:?}", OssProvider::ALI);
            println!("{:?}", OssProvider::JD);
            println!("{:?}", OssProvider::JRSS);
            println!("{:?}", OssProvider::HUAWEI);
        }

        if let Some(_) = parameters.subcommand_matches("task_type") {
            println!("{:?}", TaskType::Download);
            println!("{:?}", TaskType::LocalToLocal);
            println!("{:?}", TaskType::Transfer);
            println!("{:?}", TaskType::TruncateBucket);
            println!("{:?}", TaskType::Upload);
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
            oss_ali.provider = OssProvider::ALI;
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
        let file_size = match gen_file.get_one::<String>("file_size") {
            Some(s) => {
                let size = byte_size_to_usize(s);
                match size {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                }
            }
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

    if let Some(gen_file) = matches.subcommand_matches("gen_files") {
        let dir = match gen_file.get_one::<String>("dir") {
            Some(s) => s,
            None => {
                return;
            }
        };
        let file_prefix_len: usize = match gen_file.get_one("file_prefix_len") {
            Some(s) => *s,
            None => {
                return;
            }
        };

        let file_size = match gen_file.get_one::<String>("file_size") {
            Some(s) => {
                let size = byte_size_to_usize(s);
                match size {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!("{}", e);
                        return;
                    }
                }
            }
            None => {
                return;
            }
        };

        let file_quantity: usize = match gen_file.get_one("file_quantity") {
            Some(s) => *s,
            None => {
                return;
            }
        };

        if let Err(e) = generate_files(dir.as_str(), file_prefix_len, file_size, file_quantity) {
            log::error!("{}", e);
        };
    }
}
