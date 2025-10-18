from __future__ import annotations

from collections import defaultdict, deque
from itertools import groupby
from typing import Iterable, List, Tuple
from uuid import UUID

from sqlalchemy.orm import Session

from app.models import (
    DataObject,
    DataObjectDependency,
    Table,
    TableDependency,
    TableLoadOrder,
)


class DAGBuilder:
    """Utility class that produces topological orders for dependencies."""

    def __init__(self, db: Session):
        self.db = db

    def build_data_object_order(self) -> List[dict]:
        data_objects = self.db.query(DataObject).all()
        dependencies = self.db.query(DataObjectDependency).all()
        nodes = [(obj.id, obj.name) for obj in data_objects]
        edges = [(dep.predecessor_id, dep.successor_id) for dep in dependencies]
        return self._build_order(nodes, edges)

    def build_table_order(self) -> List[dict]:
        tables = self.db.query(Table).all()
        dependencies = self.db.query(TableDependency).all()
        load_orders = (
            self.db.query(TableLoadOrder)
            .order_by(TableLoadOrder.data_object_id, TableLoadOrder.sequence)
            .all()
        )

        nodes = [(tbl.id, f"{tbl.name}") for tbl in tables]
        edges: set[Tuple[UUID, UUID]] = {
            (dep.predecessor_id, dep.successor_id)
            for dep in dependencies
            if dep.predecessor_id != dep.successor_id
        }

        tables_by_data_object: dict[UUID, list[TableLoadOrder]] = {
            data_object_id: list(group)
            for data_object_id, group in groupby(
                load_orders, key=lambda order: order.data_object_id
            )
        }

        table_ids = {tbl.id for tbl in tables}
        for orders in tables_by_data_object.values():
            ordered_ids = [order.table_id for order in orders if order.table_id in table_ids]
            for predecessor_id, successor_id in zip(ordered_ids, ordered_ids[1:]):
                if predecessor_id != successor_id:
                    edges.add((predecessor_id, successor_id))

        return self._build_order(nodes, edges)

    def _build_order(
        self,
        nodes: Iterable[Tuple[UUID, str]],
        edges: Iterable[Tuple[UUID, UUID]],
    ) -> List[dict]:
        indegree: dict[UUID, int] = {}
        name_map: dict[UUID, str] = {}
        adjacency: dict[UUID, set[UUID]] = defaultdict(set)

        for node_id, name in nodes:
            indegree[node_id] = 0
            name_map[node_id] = name

        for predecessor_id, successor_id in edges:
            if predecessor_id == successor_id:
                continue
            if successor_id not in indegree:
                # Detached dependency reference; skip to keep builder robust
                continue
            if successor_id not in adjacency[predecessor_id]:
                adjacency[predecessor_id].add(successor_id)
                indegree[successor_id] = indegree.get(successor_id, 0) + 1

        queue: deque[UUID] = deque(
            sorted((node_id for node_id, deg in indegree.items() if deg == 0), key=lambda x: name_map[x])
        )
        order: List[dict] = []
        processed = 0

        while queue:
            current = queue.popleft()
            order.append({"id": current, "name": name_map[current], "order": processed})
            processed += 1

            for successor in sorted(adjacency[current]):
                indegree[successor] -= 1
                if indegree[successor] == 0:
                    queue.append(successor)

        if processed != len(indegree):
            raise ValueError("Dependency graph contains a cycle or unresolved references")

        return order
