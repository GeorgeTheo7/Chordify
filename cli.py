import click

@click.group()
def main_cli():
    pass

@click.command()
def join():
    click.echo("New node joined")
    raise NotImplementedError

@click.command()
@click.argument('key')
def query(key):
    click.echo("Query for key {}".format(key))
    raise NotImplementedError

@click.command()
@click.argument('key')
@click.argument('value')
def insert(key, value):
    click.echo("Insert key {} with value {}".format(key,value))
    raise NotImplementedError

@click.command()
@click.argument('key')
def delete(key):
    click.echo("Delete key {}".format(key))
    raise NotImplementedError

@click.command()
def depart():
    click.echo("Departure of node")
    raise NotImplementedError

@click.command()
def overlay():
    click.echo("Overlay")
    raise NotImplementedError

@click.command()
def help():
    with click.Context(main_cli) as ctx:
        click.echo(main_cli.get_help(ctx))

main_cli.add_command(join)
main_cli.add_command(query)
main_cli.add_command(insert)
main_cli.add_command(delete)
main_cli.add_command(depart)
main_cli.add_command(overlay)
main_cli.add_command(help)

if __name__ == "__main__":
    main_cli()