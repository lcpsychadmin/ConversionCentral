from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class DbtArtifactFile(BaseModel):
    path: str
    content: str


class DbtArtifactBundle(BaseModel):
    files: List[DbtArtifactFile]
    warnings: Optional[List[str]] = None

    def file_map(self) -> dict[str, str]:
        """Convenience mapping of file paths to content."""
        return {artifact.path: artifact.content for artifact in self.files}
