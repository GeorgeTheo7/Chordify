import click
from pyfiglet import Figlet
import subprocess as sp

def main():
    f = Figlet(font='slant')
    click.echo(f.renderText('D O B R I F L Y'))
    click.echo("Welcome to our Chord implementation!\n")
    while True:
        try:
            command = click.prompt(click.style("chord-cli@ntua",fg='bright_cyan'),type=str,prompt_suffix=': ')
            sp.run(['python','./cli.py'] + command.split(' '))
            if command == "depart":
                click.echo("\n\nExiting chord cli")
                exit(0)
        except click.Abort:
            # Need to call depart command
            click.echo("\n\nExiting chord cli")
            exit(0)

if __name__ == "__main__":
    main()