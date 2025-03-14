#!/usr/bin/env python

import click
import cmd
from pyfiglet import Figlet
import socket
import sys
import requests
import shlex
import os

import cli

class ChordifyShell(cmd.Cmd):

    prompt = click.style("chordify-cli@NTUA",fg='bright_cyan') + "$ "

    def do_help(self, line):
        # Don't use default help of cmd library
        self.default("help " + line)

    def do_exit(self, line=""):
        #Not working for edge case
        self.default("exit " + line)
        click.echo("Exiting chordify shell...")
        return True

    def default(self, line):
        
        try:
            args = shlex.split(line)
        except ValueError:
            click.echo("Ensure that your keys are formated in a correct way.")
            return cmd.Cmd.default(self, line)

        subcommand = cli.cli_group.commands.get(args[0])

        if subcommand:
            try:
                subcommand.main(args[1:],prog_name=args[0],standalone_mode = False)
            except click.NoSuchOption as option_error:
                option_error.show()
            except click.BadOptionUsage as bad_option:
                bad_option.show()
            except click.BadArgumentUsage as bad_argument:
                bad_argument.show()
            except click.UsageError as usage_error:
                usage_error.show()
            except click.BadParameter as bad_parameter:
                bad_parameter.show()
            except click.FileError as file_error:
                file_error.show()
            except click.Abort:
                click.echo("Abort. Command ended unexpectedly")
            except NotImplementedError:
                click.echo("Not Implemented Error")

        else:
            return cmd.Cmd.default(self, line)

    # For using Ctrl-D as exit shortcut
    do_EOF = do_exit

def port_in_use(ip, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((ip, port)) == 0

def start_server(kappa, consistency_type):
    ip = socket.gethostbyname(socket.gethostname())
    # Find available port
    port = None
    for p in range(5000,5150):
        if port_in_use(ip, p) == 0:
            port = p
            break

    if port == None:
        click.echo("Couldn't find available port for chordify server.")
        click.echo("Please try again later. Exit with ctrl + C")
        return False

    # Set environment variables for cli commands
    os.environ['CHORDIFYSERVER_IP'] = ip
    os.environ['CHORDIFYSERVER_PORT'] = str(port)

    pid = os.fork()
    if pid == 0:
        os.execle("./server.py","server.py",str(port),str(kappa),consistency_type,os.environ)
        # The following should never get executed:
        click.echo("Couldn't start chordify server")     
    else:
        url = "http://{}:{}/".format(ip,port)
        while True:
            try:
                r = requests.get(url)
                break
            except requests.exceptions.ConnectionError:
                pass
        click.echo(r.text)
    
    return True

def check_and_return_chordify_parameters():

    if len(sys.argv) < 2:
        return 1,""

    try:
        kappa = int(sys.argv[1])
    except ValueError:
        click.echo("replication factor must be a positive integer!")
        exit()

    if kappa <= 0:
        click.echo("replication factor must be a positive integer!")
        exit()
    elif kappa == 1:
        return kappa,""
    elif len(sys.argv) < 3:
        click.echo("Please, provide a consistency policy:")
        click.echo("EITHER chain-replication OR eventual-consistency")
        exit()
    else:
        consistency_type = sys.argv[2]
        if not consistency_type in {"chain-replication","eventual-consistency"}:
            click.echo("Not supported policy!")
            click.echo("Choose EITHER chain-replication OR eventual-consistency")
            exit()
        return kappa,consistency_type

def main():

    kappa, consistency_type = check_and_return_chordify_parameters()

    f = Figlet(font='3-d')
    click.echo(f.renderText('CHORDIFY'))
    click.echo("Welcome to Our Chord Implementation!!\n")

    if not start_server(kappa, consistency_type):
        exit() 
    chordifyshell = ChordifyShell()
    try:
        chordifyshell.cmdloop()
    except KeyboardInterrupt:
        chordifyshell.do_exit()

if __name__ == "__main__":
    main()
