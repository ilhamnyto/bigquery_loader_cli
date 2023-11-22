import typer
import getpass
import asyncio
import inquirer
from rich import print
from src.usecase.bq_loader import BigQueryLoader

async def main():
    typer.secho("Welcome to BigQuery Loader!", fg=typer.colors.BRIGHT_YELLOW)
    typer.secho("Now you can load your data from your database to BigQuery!", fg=typer.colors.BRIGHT_YELLOW)
    typer.secho(". . . . . . . . . . . . . . . . . . . . . . . . . . . .", fg=typer.colors.BRIGHT_YELLOW)

    selected_db = inquirer.list_input("Choose source database?", choices=['MySQL'])
    typer.secho(f"Please input your {selected_db} database credentials.", fg=typer.colors.BRIGHT_YELLOW)
    db_credentials = {}
    print(f"[bold yellow]Host: [/bold yellow]", end=' ')
    db_credentials['host'] = input()
    print(f"[bold yellow]Port: [/bold yellow]", end=' ')
    db_credentials['port'] = input() or 0
    print(f"[bold yellow]User: [/bold yellow]", end=' ')
    db_credentials['user'] = input()
    print(f"[bold yellow]Password: [/bold yellow]", end=' ')
    db_credentials['password'] = getpass.getpass("")
    print(f"[bold yellow]Database: [/bold yellow]", end=' ')
    db_credentials['db'] = input()
    await BigQueryLoader(selected_db, db_credentials).run()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Thank you. 👏")