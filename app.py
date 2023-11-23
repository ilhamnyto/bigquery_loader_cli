import typer
import asyncio
from rich import print
from src.usecase.bq_loader import BigQueryLoader

async def main():
    typer.secho("Welcome to BigQuery Loader!", fg=typer.colors.BRIGHT_YELLOW)
    typer.secho("Now you can load your data from your database to BigQuery!", fg=typer.colors.BRIGHT_YELLOW)
    typer.secho(". . . . . . . . . . . . . . . . . . . . . . . . . . . .", fg=typer.colors.BRIGHT_YELLOW)
    
    await BigQueryLoader().run_cli()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Thank you. 👏")