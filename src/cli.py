#!/usr/bin/env python

import click
import requests
import socket
from pathlib import Path
import os
import time
from prettytable import PrettyTable

CONTEXT_SETTINGS = dict(help_option_names=['--help','-h'])

def chordify_server_addr():
    ip = os.environ.get("CHORDIFYSERVER_IP")
    port = os.environ.get("CHORDIFYSERVER_PORT")

    if ip == None or port == None:
        raise click.Abort
    else:
        return ip, int(port)

@click.group(add_help_option=False,options_metavar="",subcommand_metavar="COMMAND [OPTIONS] [ARGS]")
def cli_group():
    pass

@cli_group.command(context_settings=CONTEXT_SETTINGS)
@click.option('-b','--bootstrap-node','bnode',required=False, nargs=2,type=str,metavar='<ip> <port>', help='Specify bootstrap node of chord')
def join(bnode):
    """
        Inserts a new node.
    """
    ip, port = chordify_server_addr()
    home_dir = str(Path.home()) + '/'
    
    url = "http://{}:{}/".format(ip, port)
    print(f"[DEBUG] bnode: {bnode}")  # Log the value of bnode

    # Change the condition to check properly if bnode is provided
    if bnode is not None:
        print(f"[DEBUG] Using provided bootstrap node: {bnode}")
        try:
            with open(home_dir + ".chordifyserver.cfg", "w") as f:
                f.write("{} {}".format(bnode[0], bnode[1]))
            print("[DEBUG] Written bootstrap node info to .chordifyserver.cfg")
        except Exception as e:
            print(f"[ERROR] Failed to write to .chordifyserver.cfg: {e}")
        params = {'ip': bnode[0], 'port': bnode[1]}
    else:
        print("[DEBUG] No bootstrap node provided, checking .chordifyserver.cfg")
        if os.path.exists(home_dir + ".chordifyserver.cfg"):
            try:
                with open(home_dir + ".chordifyserver.cfg", "r") as f:
                    line = f.readline().split()
                    print(f"[DEBUG] Read from .chordifyserver.cfg: {line}")
                    if not line:
                        click.echo("Please provide a bootstrap node")
                        return
                    bnode_ip = line[0]
                    bnode_port = line[1]
                params = {'ip': bnode_ip, 'port': bnode_port}
            except Exception as e:
                print(f"[ERROR] Failed to read .chordifyserver.cfg: {e}")
                return
        else:
            click.echo("Please provide a bootstrap node")
            return

    try:
        print(f"[DEBUG] Sending join request to {url} with params: {params}")
        r = requests.put(url + "join", params=params)
        print(f"[DEBUG] Server responded: {r.text}")
        click.echo(r.text)
    except Exception as e:
        print(f"[ERROR] Failed to send join request: {e}")



@cli_group.command(context_settings=CONTEXT_SETTINGS)
@click.argument('key', metavar='<key>')
def query(key):
    """
        Finds the value of <key>.
    """
    ip, port = chordify_server_addr()

    if key == "*":

        url = "http://{}:{}/queryAll".format(ip,port)
        r = requests.get(url)

        if r.status_code == 200:
            data = r.json()
            for node in data:

                click.echo("Node Info:")
                click.echo(f"* Node Hash: {node['node']['hash']}")
                click.echo(f"* Node IP: {node['node']['ip']}")
                click.echo(f"* Node Port: {node['node']['port']}")
                click.echo()

                click.echo("Primary Keys:")
                t1 = PrettyTable()
                t1.field_names = ["Hash", "Key", "Value"]
                for k in node["keys"]:
                    t1.add_row(list(k.values()))
                print(t1)

                click.echo()
                click.echo("Replicas Keys:")
                t2 = PrettyTable()
                t2.field_names = ["Hash", "Key", "Value", "Replica Number"]
                for k in node["replicas"]:
                    t2.add_row(list(k.values()))
                print(t2)
                click.echo()
    else:
        url = "http://{}:{}/query".format(ip,port)
        r = requests.get(url, params={"key":key})
        if r.status_code == 200:
            t1 = PrettyTable()
            t1.field_names = ["Hash", "Key", "Value","Replica Number","Node IP", "Node Port"]
            t1.add_row(list(r.json().values()))
            print(t1)
        else:
            click.echo(r.text)


@cli_group.command(context_settings=CONTEXT_SETTINGS)
@click.argument('key', metavar='<key>')
@click.argument('value', metavar='<value>')
def insert(key, value):
    """
        Inserts the pair (<key>, <value>).
    """
    ip, port = chordify_server_addr()

    url = "http://{}:{}/insert".format(ip,port)
    r = requests.post(url, params={"key":key,"value":value})
    if r.status_code == 200:
        t1 = PrettyTable()
        t1.field_names = ["Hash", "Key", "Value","Node IP", "Node Port"]
        t1.add_row(list(r.json().values()))
        print(t1)
        click.echo()
        click.echo("Key inserted successfully.")
    else:
        click.echo(r.text)

@cli_group.command(context_settings=CONTEXT_SETTINGS)
@click.argument('key', metavar='<key>')
def delete(key):
    """
        Deletes the specified <key>.
    """
    ip, port = chordify_server_addr()

    url = "http://{}:{}/delete".format(ip,port)
    r = requests.delete(url, params={"key":key})
    if r.status_code == 200:
        t1 = PrettyTable()
        t1.field_names = ["Hash", "Key", "Value","Node IP", "Node Port"]
        t1.add_row(list(r.json().values()))
        print(t1)
        click.echo()
        click.echo("Key deleted successfully.")
    else:
        click.echo(r.text)

@cli_group.command(context_settings=CONTEXT_SETTINGS)
def depart():
    """
        Makes current node to depart.
    """
    ip, port = chordify_server_addr()

    url = "http://{}:{}/depart".format(ip,port)
    r = requests.delete(url)
    click.echo(r.text)

@cli_group.command(context_settings=CONTEXT_SETTINGS)
def exit():
    """
        Makes current node to depart & exits from shell.
    """
    ip, port = chordify_server_addr()

    url = "http://{}:{}/shutdown".format(ip,port)
    r = requests.post(url)
    click.echo(r.text)

@cli_group.command(context_settings=CONTEXT_SETTINGS)
def overlay():
    """
        Displays current network topology.
    """
    ip, port = chordify_server_addr()

    url = "http://{}:{}/overlay".format(ip,port)
    r = requests.get(url)
    
    if r.status_code == 200:

        data = r.json()

        click.echo("Cluster Info:")
        t1 = PrettyTable()
        t1.field_names = ["Hash", "Node IP", "Node Port"]
        data["nodes"].sort(key = lambda d: d["node_key"])
        for d in data["nodes"]:
            if d["ip"] == ip and d["port"] == port:
                t1.add_row(list(map(lambda x: click.style(str(x),fg='green'), list(d.values()))))
            else:
                t1.add_row(list(d.values()))
        print(t1)
    else:
        click.echo(r.text)

@cli_group.command(context_settings=CONTEXT_SETTINGS)
def info():
    """
        Displays info for current node.
    """
    ip, port = chordify_server_addr()

    url = "http://{}:{}/info".format(ip,port)
    r = requests.get(url)
    if r.status_code == 200:

        data = r.json()

        click.echo("Node Info:")
        click.echo(f"* Node IP: {ip}")
        click.echo(f"* Node Port: {port}")
        click.echo()

        click.echo("Primary Keys:")
        t1 = PrettyTable()
        t1.field_names = ["Hash", "Key", "Value"]
        for k in data["keys"]:
            t1.add_row(list(k.values()))
        print(t1)

        click.echo()
        click.echo("Replicas Keys:")
        t2 = PrettyTable()
        t2.field_names = ["Hash", "Key", "Value", "Replica Number"]
        for k in data["replicas"]:
            t2.add_row(list(k.values()))
        print(t2)

        click.echo()
        click.echo("Connected Nodes:")
        t3 = PrettyTable()
        t3.field_names = ["Hash", "IP", "Port", "Role"]

        if data["previous"] != {}:
            t3.add_row([*data["previous"].values(), "Previous Node"])

        if data["next"] != {}:
            t3.add_row([*data["next"].values(), "Next Node"])
        print(t3)

    else:
        click.echo(r.text)

#   Dummy command, just for
#   showing up in cli help message
@cli_group.command(context_settings=CONTEXT_SETTINGS, short_help="Prints this message and exits.")
def help():
    """
        Prints a help message for all chordify commands.
    """
    with click.Context(cli_group) as ctx:
        click.echo(cli_group.get_help(ctx))
    return 0

if __name__ == "__main__":
    cli_group()
