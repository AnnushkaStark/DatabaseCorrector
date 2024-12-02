import asyncio

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import Column, Index, Table


class DatabaseCorrector:
    def __init__(self, source_url: str, target_url: str):
        self.source_engine = create_engine(source_url)
        self.target_engine = create_engine(target_url)

    async def compare_schemas(self):
        source_inspector = inspect(self.source_engine)
        target_inspector = inspect(self.target_engine)
        differences = []
        # Сравниваем таблицы
        for table_name in set(source_inspector.get_table_names()) | set(
            target_inspector.get_table_names()
        ):
            try:
                source_table = source_inspector.get_table(table_name)
                target_table = target_inspector.get_table(table_name)
                if source_table is None:
                    differences.append(f"Добавлена таблица {table_name}")
                elif target_table is None:
                    differences.append(f"Отсутствует таблица {table_name}")
                else:
                    # Сравниваем колонки
                    for col in source_table.columns:
                        if col.name not in [
                            c.name for c in target_table.columns
                        ]:
                            differences.append(
                                f"В таблице {table_name} отсутствует колонка {col.name}"  # noqa: E501
                            )
                    # Сравниваем индексы
                    for idx in source_table.indexes:
                        if idx.name not in [
                            i.name for i in target_table.indexes
                        ]:
                            differences.append(
                                f"В таблице {table_name} отсутствует индекс {idx.name}"  # noqa: E501
                            )
            except OperationalError:
                continue
        return differences

    async def correct_schema(self):
        differences = await self.compare_schemas()
        for diff in differences:
            print(f"Корректируем: {diff}")
            if "Добавлена таблица" in diff:
                table_name = diff.split(" ")[-1]
                await self.add_table(table_name)
            elif "Отсутствует таблица" in diff:
                table_name = diff.split(" ")[-1]
                await self.create_table(table_name)
            elif "отсутствует колонка" in diff:
                table_name, col_name = diff.split(" ")[-2:]
                await self.add_column(table_name, col_name)
            elif "отсутствует индекс" in diff:
                table_name, idx_name = diff.split(" ")[-2:]
                await self.add_index(table_name, idx_name)
        print("Корректировка завершена")

    async def add_table(self, table_name: str):
        inspector = inspect(self.source_engine)
        table = inspector.get_table(table_name)
        if table is None:
            print(f"Таблица {table_name} не найдена в исходной базе данных")
            return
        columns = [Column(col.name, col.type) for col in table.columns]
        indexes = [Index(idx.name, *[idx.columns]) for idx in table.indexes]
        new_table = Table(table_name, self.metadata, *columns, *indexes)
        async with self.target_engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: new_table.create(sync_conn))
        print(f"Таблица {table_name} добавлена")

    async def create_table(self, table_name: str):
        inspector = inspect(self.source_engine)
        table = inspector.get_table(table_name)
        if table is None:
            print(f"Таблица {table_name} не найдена в исходной базе данных")
            return
        columns = [Column(col.name, col.type) for col in table.columns]
        indexes = [Index(idx.name, *[idx.columns]) for idx in table.indexes]
        new_table = Table(table_name, self.metadata, *columns, *indexes)
        async with self.target_engine.begin() as conn:
            await conn.run_sync(lambda sync_conn: new_table.create(sync_conn))
        print(f"Таблица {table_name} создана")

    async def add_column(self, table_name: str, col_name: str):
        inspector = inspect(self.source_engine)
        table = inspector.get_table(table_name)
        if table is None:
            print(f"Таблица {table_name} не найдена в исходной базе данных")
            return
        col = next((c for c in table.columns if c.name == col_name), None)
        if col is None:
            print(f"Колонка {col_name} не найдена в таблице {table_name}")
            return
        async with self.target_engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                ALTER TABLE {table_name}
                ADD COLUMN {col.compile(dialect=self.target_engine.dialect)}
            """
                )
            )
        print(f"Колонка {col_name} добавлена в таблицу {table_name}")

    async def add_index(self, table_name: str, idx_name: str):
        inspector = inspect(self.source_engine)
        table = inspector.get_table(table_name)
        if table is None:
            print(f"Таблица {table_name} не найдена в исходной базе данных")
            return
        idx = next((i for i in table.indexes if i.name == idx_name), None)
        if idx is None:
            print(f"Индекс {idx_name} не найден в таблице {table_name}")
            return
        async with self.target_engine.begin() as conn:
            await conn.execute(
                text(
                    f"""
                CREATE INDEX {idx_name}
                ON {table_name} ({', '.join(col.name for col in idx.columns)})
            """
                )
            )

        print(f"Индекс {idx_name} добавлен в таблицу {table_name}")


async def main():
    source_url = "postgresql://user:password@host/source_db"
    target_url = "postgresql://user:password@host/target_db"

    corrector = DatabaseCorrector(source_url, target_url)
    await corrector.correct_schema()


if __name__ == "__main__":
    asyncio.run(main())
