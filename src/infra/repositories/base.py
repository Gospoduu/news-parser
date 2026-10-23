from typing import Any, TypeVar
from uuid import UUID

from pydantic.main import BaseModel
from sqlalchemy import  insert, select, delete, Table
from sqlalchemy.ext.asyncio import AsyncConnection

ModelType = TypeVar('ModelType', bound=BaseModel)


class BaseRepository:
    """Базовый репозиторий."""

    model: Table
    schema_class: type[ModelType]

    def from_row(self, row: Any) -> ModelType:
        return self.schema_class.model_validate(row._mapping)

    async def create(self, connection: AsyncConnection, **kwargs: Any) -> ModelType:
        """Создание объекта.

        Args:
            kwargs: Параметры для создания нового объекта.

        Returns:
            ModelType: Созданный объект.
        """
        statement = insert(self.model).values(**kwargs).returning(self.model)  # type: ignore
        model_data = await connection.execute(statement)  # type: ignore
        row = model_data.fetchone()
        return self.from_row(row)

    async def get_one_by_id(self, connection: AsyncConnection, id: int | UUID) -> ModelType | None:
        query = select(self.model).where(self.model.c.id == id)
        result = await connection.execute(query)
        row = result.fetchone()
        if row is None:
            return None
        return self.from_row(row)

    async def delete_by_id(self, connection: AsyncConnection, id: int | UUID) -> bool:
        query = delete(self.model).where(self.model.id == id)
        result = await connection.execute(query)
        return result.rowcount == 1

