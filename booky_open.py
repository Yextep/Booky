#!/usr/bin/env python3
"""
Booky Open 2 — buscador/descargador de documentos abiertos.

Esta versión evita el problema típico de Internet Archive: hay ítems que muestran
archivos en sus metadatos, pero sus binarios están restringidos y devuelven 401/403.
Booky ahora valida enlaces directos antes de marcarlos como descargables y, si un
archivo no está disponible, abre la ficha pública en vez de intentar saltarse permisos.

Fuentes incluidas:
- Project Gutenberg vía Gutendex: libros de dominio público.
- arXiv: preprints/papers de acceso abierto en PDF.
- DOAB: libros académicos open access con bitstreams oficiales.
- Europe PMC + PMC OA service: artículos biomédicos open access con PDF cuando existe.
- Internet Archive: sólo descargas directas verificadas; ítems restringidos quedan como ficha.
- Open Library: catálogo/metadatos y disponibilidad, sin descarga automática.
- OpenAlex: opcional si defines OPENALEX_API_KEY; busca obras OA con pdf_url/content_url.

Uso:
    python booky_open.py

Dependencias:
    pip install -r requirements.txt
"""

from __future__ import annotations

import csv
import hashlib
import json
import mimetypes
import os
import re
import sys
import time
import unicodedata
import webbrowser
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote, quote_plus, urljoin, urlparse
from xml.etree import ElementTree as ET

import requests
from requests import Response, Session
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.prompt import Confirm, IntPrompt, Prompt
from rich.table import Table

APP_NAME = "Booky Open 2"
USER_AGENT = "BookyOpen/2.0 (+https://example.local; open-access document discovery)"

DEFAULT_FORMATS = ["pdf", "epub", "txt"]
ALL_FORMATS = ["pdf", "epub", "txt", "html", "mobi", "doc", "docx", "tgz"]

DEFAULT_SOURCES = ["gutenberg", "arxiv", "doab", "europe_pmc", "internet_archive", "open_library"]
ALL_SOURCES = [
    "gutenberg",
    "arxiv",
    "doab",
    "europe_pmc",
    "internet_archive",
    "open_library",
    "openalex",
]

console = Console()


@dataclass
class SearchConfig:
    formats: list[str] = field(default_factory=lambda: DEFAULT_FORMATS.copy())
    sources: list[str] = field(default_factory=lambda: DEFAULT_SOURCES.copy())
    language: str = ""  # ISO-639-1, por ejemplo: es, en, fr. Vacío = sin filtro fuerte.
    limit_per_source: int = 10
    download_dir: Path = Path("Booky_Documentos")
    max_download_mb: int = 250
    verify_links: bool = True
    include_metadata_only: bool = True


@dataclass
class UrlProbe:
    ok: bool
    status_code: int = 0
    final_url: str = ""
    content_type: str = ""
    content_length: Optional[int] = None
    reason: str = ""


@dataclass
class DocumentResult:
    title: str
    source: str
    authors: list[str] = field(default_factory=list)
    year: str = ""
    language: str = ""
    fmt: str = "metadata"
    download_url: str = ""
    source_url: str = ""
    license: str = ""
    description: str = ""
    access: str = "metadata"  # verified, unverified, metadata, restricted, dead, too_large
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def downloadable(self) -> bool:
        return bool(self.download_url) and self.access not in {"metadata", "restricted", "dead", "too_large"}

    @property
    def short_authors(self) -> str:
        if not self.authors:
            return "—"
        joined = ", ".join(self.authors[:2])
        if len(self.authors) > 2:
            joined += "…"
        return joined

    @property
    def access_label(self) -> str:
        labels = {
            "verified": "✅ directo",
            "unverified": "⚠ sin verificar",
            "metadata": "🔎 ficha",
            "restricted": "⛔ restringido",
            "dead": "❌ caído",
            "too_large": "📦 muy grande",
        }
        return labels.get(self.access, self.access or "—")


class BookyError(Exception):
    """Error controlado para mostrar mensajes amigables."""


# ---------------------------------------------------------------------------
# Utilidades generales
# ---------------------------------------------------------------------------


def create_session() -> Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json, application/xml;q=0.9, text/xml;q=0.8, */*;q=0.7",
    })
    return session


def request_json(
    session: Session,
    url: str,
    *,
    params: Optional[Any] = None,
    timeout: tuple[int, int] = (10, 45),
    headers: Optional[dict[str, str]] = None,
) -> dict[str, Any] | list[Any]:
    response = session.get(url, params=params, timeout=timeout, headers=headers)
    response.raise_for_status()
    return response.json()


def normalize_spaces(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def safe_filename(text: str, fallback: str = "documento") -> str:
    text = normalize_spaces(text) or fallback
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\\/:*?\"<>|\r\n\t]+", "_", text)
    text = re.sub(r"\s+", " ", text).strip(" ._")
    return text[:140] or fallback


def short_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:10]


def clean_query(text: str) -> str:
    text = normalize_spaces(text)
    return re.sub(r"[\x00-\x1f\x7f]", "", text)


def coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def first_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, list):
            for item in value:
                text = normalize_spaces(item)
                if text:
                    return text
        else:
            text = normalize_spaces(value)
            if text:
                return text
    return ""


def truthy_text(value: Any) -> bool:
    return normalize_spaces(value).lower() in {"1", "true", "yes", "y", "si", "sí"}


def parse_format_list(text: str) -> list[str]:
    values = [item.strip().lower().lstrip(".") for item in re.split(r"[,\s]+", text) if item.strip()]
    valid: list[str] = []
    for value in values:
        if value not in ALL_FORMATS:
            raise BookyError(f"Formato no soportado: {value}. Usa: {', '.join(ALL_FORMATS)}")
        if value not in valid:
            valid.append(value)
    return valid or DEFAULT_FORMATS.copy()


def parse_source_list(text: str) -> list[str]:
    aliases = {
        "ia": "internet_archive",
        "archive": "internet_archive",
        "internetarchive": "internet_archive",
        "internet_archive": "internet_archive",
        "gutenberg": "gutenberg",
        "gutendex": "gutenberg",
        "pg": "gutenberg",
        "arxiv": "arxiv",
        "doab": "doab",
        "oapen": "doab",
        "europepmc": "europe_pmc",
        "europe_pmc": "europe_pmc",
        "pmc": "europe_pmc",
        "pubmedcentral": "europe_pmc",
        "openlibrary": "open_library",
        "open_library": "open_library",
        "ol": "open_library",
        "openalex": "openalex",
        "alex": "openalex",
    }
    selected: list[str] = []
    for raw in re.split(r"[,\s]+", text.strip().lower()):
        if not raw:
            continue
        mapped = aliases.get(raw)
        if not mapped:
            raise BookyError(
                "Fuente no reconocida. Usa: gutenberg, arxiv, doab, europepmc, ia, openlibrary, openalex"
            )
        if mapped not in selected:
            selected.append(mapped)
    return selected or DEFAULT_SOURCES.copy()


def parse_indices(text: str, max_index: int) -> list[int]:
    out: list[int] = []
    for piece in text.split(","):
        piece = piece.strip()
        if not piece:
            continue
        if "-" in piece:
            start_s, end_s = piece.split("-", 1)
            start, end = int(start_s), int(end_s)
            if start > end:
                start, end = end, start
            out.extend(range(start, end + 1))
        else:
            out.append(int(piece))

    unique: list[int] = []
    for idx in out:
        if idx < 1 or idx > max_index:
            raise BookyError(f"Índice fuera de rango: {idx}")
        if idx not in unique:
            unique.append(idx)
    return unique


def dedupe_results(results: Iterable[DocumentResult]) -> list[DocumentResult]:
    seen: set[str] = set()
    unique: list[DocumentResult] = []
    for result in results:
        key = (result.download_url or result.source_url or f"{result.source}:{result.title}:{result.fmt}").lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(result)
    return unique


def extension_from_url(url: str) -> str:
    path = urlparse(url).path.lower()
    for ext in ALL_FORMATS:
        if path.endswith(f".{ext}") or f".{ext}." in path:
            return ext
    guessed, _ = mimetypes.guess_type(path)
    if guessed:
        if guessed == "application/pdf":
            return "pdf"
        if guessed == "application/epub+zip":
            return "epub"
        if guessed.startswith("text/"):
            return "txt"
        if guessed == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            return "docx"
        if guessed == "application/msword":
            return "doc"
        if guessed in {"application/gzip", "application/x-gzip"}:
            return "tgz"
    return "bin"


def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    parent = path.parent
    for i in range(2, 1000):
        candidate = parent / f"{stem} ({i}){suffix}"
        if not candidate.exists():
            return candidate
    return parent / f"{stem}-{short_hash(str(time.time()))}{suffix}"


def ftp_to_https(url: str) -> str:
    if url.startswith("ftp://ftp.ncbi.nlm.nih.gov/"):
        return "https://ftp.ncbi.nlm.nih.gov/" + url[len("ftp://ftp.ncbi.nlm.nih.gov/") :]
    return url


# ---------------------------------------------------------------------------
# Validación de URLs directas
# ---------------------------------------------------------------------------


def expected_mimes(fmt: str) -> set[str]:
    return {
        "pdf": {"application/pdf"},
        "epub": {"application/epub+zip", "application/octet-stream"},
        "txt": {"text/plain", "text/markdown", "application/octet-stream"},
        "html": {"text/html", "application/xhtml+xml"},
        "mobi": {"application/x-mobipocket-ebook", "application/octet-stream"},
        "doc": {"application/msword", "application/octet-stream"},
        "docx": {"application/vnd.openxmlformats-officedocument.wordprocessingml.document", "application/octet-stream"},
        "tgz": {"application/gzip", "application/x-gzip", "application/octet-stream"},
    }.get(fmt, {"application/octet-stream"})


def content_type_matches(fmt: str, url: str, content_type: str) -> bool:
    content_type = (content_type or "").split(";", 1)[0].strip().lower()
    if not content_type:
        return True
    if content_type in expected_mimes(fmt):
        return True
    if fmt == "txt" and content_type.startswith("text/"):
        return True
    if fmt == "html" and content_type.startswith("text/html"):
        return True
    # Algunos servidores devuelven octet-stream aunque el nombre sea correcto.
    if content_type == "application/octet-stream" and extension_from_url(url) == fmt:
        return True
    return False


def parse_content_length(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def evaluate_probe_response(response: Response, url: str, fmt: str, config: SearchConfig) -> UrlProbe:
    content_type = response.headers.get("content-type", "")
    content_length = parse_content_length(response.headers.get("content-length"))
    final_url = response.url or url
    status = response.status_code

    if status in {401, 403}:
        return UrlProbe(False, status, final_url, content_type, content_length, "requiere permisos o login")
    if status == 404:
        return UrlProbe(False, status, final_url, content_type, content_length, "archivo no encontrado")
    if status >= 400:
        return UrlProbe(False, status, final_url, content_type, content_length, f"HTTP {status}")

    if content_length is not None:
        max_bytes = config.max_download_mb * 1024 * 1024
        if content_length > max_bytes:
            return UrlProbe(False, status, final_url, content_type, content_length, "supera el tamaño máximo configurado")

    if fmt != "html" and "text/html" in content_type.lower() and extension_from_url(final_url) != fmt:
        return UrlProbe(False, status, final_url, content_type, content_length, "el servidor devolvió HTML, no un archivo directo")

    if not content_type_matches(fmt, final_url, content_type):
        return UrlProbe(False, status, final_url, content_type, content_length, f"tipo inesperado: {content_type}")

    return UrlProbe(True, status, final_url, content_type, content_length, "ok")


def probe_download_url(session: Session, url: str, fmt: str, config: SearchConfig) -> UrlProbe:
    url = ftp_to_https(url)
    headers = {"Range": "bytes=0-0", "Accept": ", ".join(sorted(expected_mimes(fmt))) + ", */*;q=0.5"}

    # 1) HEAD es rápido, pero muchos servidores lo bloquean o devuelven cabeceras pobres.
    try:
        head = session.head(url, allow_redirects=True, timeout=(8, 20), headers={"Accept": headers["Accept"]})
        if head.status_code not in {405, 501}:
            head_probe = evaluate_probe_response(head, url, fmt, config)
            if head_probe.ok or head.status_code in {401, 403, 404}:
                return head_probe
    except requests.RequestException:
        pass

    # 2) GET con Range evita descargar todo el archivo. Si el servidor ignora Range,
    #    stream=True impide cargar el cuerpo completo en memoria.
    try:
        with session.get(url, allow_redirects=True, timeout=(10, 30), headers=headers, stream=True) as response:
            return evaluate_probe_response(response, url, fmt, config)
    except requests.RequestException as exc:
        return UrlProbe(False, 0, url, "", None, str(exc))


def validate_direct_links(session: Session, results: list[DocumentResult], config: SearchConfig) -> list[DocumentResult]:
    if not config.verify_links:
        for r in results:
            if r.download_url and r.access == "unverified":
                r.access = "unverified"
        return results

    to_probe = [r for r in results if r.download_url]
    if not to_probe:
        return results

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task_id = progress.add_task("Validando enlaces directos…", total=len(to_probe))
        for result in to_probe:
            probe = probe_download_url(session, result.download_url, result.fmt, config)
            result.metadata["url_probe"] = asdict(probe)
            if probe.ok:
                result.access = "verified"
                result.download_url = probe.final_url or result.download_url
            else:
                original = result.download_url
                result.download_url = ""
                if "tamaño" in probe.reason or "supera" in probe.reason:
                    result.access = "too_large"
                elif probe.status_code in {401, 403} or "login" in probe.reason.lower() or "permis" in probe.reason.lower():
                    result.access = "restricted"
                else:
                    result.access = "dead"
                note = f"Descarga directa descartada: {probe.reason}. URL original: {original}"
                result.description = f"{result.description} {note}".strip()
            progress.update(task_id, advance=1)
    return results


# ---------------------------------------------------------------------------
# Proveedores
# ---------------------------------------------------------------------------


class SourceProvider:
    name = "base"
    key = "base"

    def __init__(self, session: Session):
        self.session = session

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        raise NotImplementedError


class InternetArchiveProvider(SourceProvider):
    name = "Internet Archive"
    key = "internet_archive"
    ADVANCED_SEARCH_URL = "https://archive.org/advancedsearch.php"
    METADATA_URL = "https://archive.org/metadata/{identifier}"
    DOWNLOAD_URL = "https://archive.org/download/{identifier}/{filename}"

    def _ia_phrase(self, text: str) -> str:
        text = clean_query(text).replace('"', " ")
        return f'"{normalize_spaces(text)}"'

    def _build_query(self, query: str) -> str:
        phrase = self._ia_phrase(query)
        # No se intenta evadir restricciones. La validación real ocurre con metadata + URL probe.
        return (
            "mediatype:texts AND "
            f"({phrase} OR title:{phrase} OR creator:{phrase} OR subject:{phrase} OR description:{phrase})"
        )

    def _item_restricted_reason(self, metadata_payload: dict[str, Any]) -> str:
        meta = metadata_payload.get("metadata", {}) or {}
        collections = [normalize_spaces(c).lower() for c in coerce_list(meta.get("collection"))]
        restricted_collections = {"inlibrary", "printdisabled", "internetarchivebooks", "china", "americana"}
        # No todo en americana/china está restringido; por eso esas colecciones no son bloqueo duro.
        hard_restricted = {"inlibrary", "printdisabled"}
        if any(c in hard_restricted for c in collections):
            return "colección de préstamo/accesibilidad, no descarga directa pública"

        for key in ["access-restricted-item", "is_dark", "noindex"]:
            if truthy_text(meta.get(key)):
                return f"metadato restrictivo: {key}={meta.get(key)}"

        lending_status = normalize_spaces(meta.get("lending___status") or meta.get("lending_status"))
        if lending_status and lending_status.lower() not in {"open", "available"}:
            return f"estado de préstamo: {lending_status}"

        return ""

    def _detect_file_format(self, file_obj: dict[str, Any]) -> str:
        name = (file_obj.get("name") or "").lower()
        fmt = (file_obj.get("format") or "").lower()
        if name.endswith(".pdf") or "pdf" in fmt:
            return "pdf"
        if name.endswith(".epub") or "epub" in fmt:
            return "epub"
        if name.endswith(".txt") or "djvutxt" in fmt or fmt in {"text", "plain text"}:
            return "txt"
        if name.endswith((".html", ".htm")) or "html" in fmt:
            return "html"
        if name.endswith(".mobi") or "mobipocket" in fmt:
            return "mobi"
        if name.endswith(".docx"):
            return "docx"
        if name.endswith(".doc"):
            return "doc"
        return "unknown"

    def _file_looks_restricted(self, file_obj: dict[str, Any]) -> bool:
        name = (file_obj.get("name") or "").lower()
        fmt = (file_obj.get("format") or "").lower()
        if truthy_text(file_obj.get("private")):
            return True
        if normalize_spaces(file_obj.get("access")).lower() in {"restricted", "private", "dark"}:
            return True
        blocked_terms = ["encrypted", "lock", "lcp", "_daisy", "daisy.zip", "limited"]
        if any(term in name or term in fmt for term in blocked_terms):
            return True
        return False

    def _score_ia_file(self, file_obj: dict[str, Any], desired_fmt: str) -> int:
        name = (file_obj.get("name") or "").lower()
        fmt = (file_obj.get("format") or "").lower()
        score = 0
        if desired_fmt == "pdf":
            if name.endswith(".pdf"):
                score += 25
            if "text pdf" in fmt:
                score += 15
            if "pdf" in fmt:
                score += 8
        elif desired_fmt == "epub":
            if name.endswith(".epub"):
                score += 25
            if "epub" in fmt:
                score += 15
        elif desired_fmt == "txt":
            if name.endswith("_djvu.txt"):
                score += 30
            if name.endswith(".txt"):
                score += 20
            if "djvutxt" in fmt:
                score += 12
        elif desired_fmt == "html":
            if name.endswith((".html", ".htm")):
                score += 25
        elif desired_fmt in {"doc", "docx", "mobi"}:
            if name.endswith(f".{desired_fmt}"):
                score += 25
        if file_obj.get("source") == "original":
            score += 3
        if self._file_looks_restricted(file_obj):
            score -= 100
        try:
            size = int(file_obj.get("size") or 0)
            if size > 0:
                score += 1
        except (TypeError, ValueError):
            pass
        return score

    def _choose_files(self, files: list[dict[str, Any]], formats: list[str]) -> dict[str, dict[str, Any]]:
        chosen: dict[str, dict[str, Any]] = {}
        scores: dict[str, int] = {}
        for file_obj in files:
            if self._file_looks_restricted(file_obj):
                continue
            detected = self._detect_file_format(file_obj)
            if detected not in formats:
                continue
            score = self._score_ia_file(file_obj, detected)
            if score <= 0:
                continue
            if detected not in chosen or score > scores[detected]:
                chosen[detected] = file_obj
                scores[detected] = score
        return chosen

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        rows = min(max(config.limit_per_source * 3, config.limit_per_source), 50)
        params = [
            ("q", self._build_query(query)),
            ("fl[]", "identifier"),
            ("fl[]", "title"),
            ("fl[]", "creator"),
            ("fl[]", "year"),
            ("fl[]", "date"),
            ("fl[]", "language"),
            ("fl[]", "downloads"),
            ("rows", str(rows)),
            ("page", "1"),
            ("output", "json"),
            ("sort[]", "downloads desc"),
        ]
        payload = request_json(self.session, self.ADVANCED_SEARCH_URL, params=params)
        assert isinstance(payload, dict)
        docs = payload.get("response", {}).get("docs", [])
        results: list[DocumentResult] = []

        for doc in docs:
            if len([r for r in results if r.source == self.name]) >= config.limit_per_source * max(1, len(config.formats)):
                break
            identifier = doc.get("identifier")
            if not identifier:
                continue
            meta_url = self.METADATA_URL.format(identifier=quote(identifier, safe=""))
            try:
                metadata_payload = request_json(self.session, meta_url, timeout=(10, 45))
                assert isinstance(metadata_payload, dict)
            except Exception as exc:  # noqa: BLE001
                console.print(f"[yellow]No pude leer metadatos de IA para {identifier}: {exc}[/yellow]")
                continue

            meta = metadata_payload.get("metadata", {}) or {}
            title = normalize_spaces(doc.get("title") or meta.get("title") or identifier)
            creators = doc.get("creator") or meta.get("creator") or []
            if isinstance(creators, str):
                creators = [creators]
            year = str(doc.get("year") or meta.get("year") or "")
            language = first_text(doc.get("language"), meta.get("language"))
            source_url = f"https://archive.org/details/{quote(identifier, safe='')}"
            restriction = self._item_restricted_reason(metadata_payload)

            if restriction:
                if config.include_metadata_only:
                    results.append(
                        DocumentResult(
                            title=title,
                            source=self.name,
                            authors=[normalize_spaces(c) for c in creators if normalize_spaces(c)],
                            year=year,
                            language=language,
                            fmt="metadata",
                            source_url=source_url,
                            description=restriction,
                            access="restricted",
                            metadata={"identifier": identifier, "restriction": restriction},
                        )
                    )
                continue

            files = metadata_payload.get("files", []) or []
            chosen = self._choose_files(files, config.formats)
            if not chosen:
                if config.include_metadata_only:
                    results.append(
                        DocumentResult(
                            title=title,
                            source=self.name,
                            authors=[normalize_spaces(c) for c in creators if normalize_spaces(c)],
                            year=year,
                            language=language,
                            fmt="metadata",
                            source_url=source_url,
                            description="No encontré archivo directo en los formatos configurados.",
                            access="metadata",
                            metadata={"identifier": identifier},
                        )
                    )
                continue

            for fmt, file_obj in chosen.items():
                filename = file_obj.get("name")
                if not filename:
                    continue
                download_url = self.DOWNLOAD_URL.format(
                    identifier=quote(identifier, safe=""),
                    filename=quote(filename, safe="/"),
                )
                results.append(
                    DocumentResult(
                        title=title,
                        source=self.name,
                        authors=[normalize_spaces(c) for c in creators if normalize_spaces(c)],
                        year=year,
                        language=language,
                        fmt=fmt,
                        download_url=download_url,
                        source_url=source_url,
                        license=str(meta.get("licenseurl") or meta.get("rights") or ""),
                        description="Archivo candidato; se valida antes de marcarlo como descargable.",
                        access="unverified",
                        metadata={"identifier": identifier, "file": file_obj},
                    )
                )
        return results[: max(config.limit_per_source, config.limit_per_source * len(config.formats))]


class GutenbergProvider(SourceProvider):
    name = "Project Gutenberg"
    key = "gutenberg"
    API_URL = "https://gutendex.com/books/"

    def _find_format_url(self, formats: dict[str, str], desired_fmt: str) -> str:
        candidates: list[tuple[int, str]] = []
        for mime, url in formats.items():
            mime_low = mime.lower()
            url_low = url.lower()
            score = 0
            if desired_fmt == "epub" and (
                "application/epub+zip" in mime_low or url_low.endswith(".epub") or ".epub" in url_low
            ):
                score = 30
                if "images" not in url_low:
                    score += 2
            elif desired_fmt == "txt" and (
                mime_low.startswith("text/plain") or url_low.endswith(".txt") or ".txt" in url_low
            ):
                score = 30
                if "utf-8" in mime_low or "utf-8" in url_low:
                    score += 3
            elif desired_fmt == "html" and (
                mime_low.startswith("text/html") or url_low.endswith((".html", ".htm"))
            ):
                score = 30
            elif desired_fmt == "mobi" and ("mobipocket" in mime_low or ".mobi" in url_low):
                score = 30
            elif desired_fmt == "pdf" and ("application/pdf" in mime_low or url_low.endswith(".pdf")):
                score = 30
            elif desired_fmt in {"doc", "docx"} and url_low.endswith(f".{desired_fmt}"):
                score = 20

            if score:
                candidates.append((score, url))
        if not candidates:
            return ""
        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        params: dict[str, Any] = {
            "search": clean_query(query),
            "copyright": "false",
            "sort": "popular",
        }
        if config.language:
            params["languages"] = config.language.lower()

        payload = request_json(self.session, self.API_URL, params=params)
        assert isinstance(payload, dict)
        books = payload.get("results", [])[: config.limit_per_source]
        results: list[DocumentResult] = []
        for book in books:
            book_id = book.get("id")
            title = normalize_spaces(book.get("title") or f"Gutenberg {book_id}")
            authors = [a.get("name", "") for a in book.get("authors", []) if a.get("name")]
            languages = ",".join(book.get("languages", []) or [])
            source_url = f"https://www.gutenberg.org/ebooks/{book_id}" if book_id else "https://www.gutenberg.org/"
            formats = book.get("formats", {}) or {}
            added = False
            for fmt in config.formats:
                url = self._find_format_url(formats, fmt)
                if not url:
                    continue
                added = True
                results.append(
                    DocumentResult(
                        title=title,
                        source=self.name,
                        authors=authors,
                        language=languages,
                        fmt=fmt,
                        download_url=url,
                        source_url=source_url,
                        license="Dominio público en EE. UU. según Gutendex/Project Gutenberg"
                        if book.get("copyright") is False
                        else "",
                        description="; ".join((book.get("subjects") or [])[:3]),
                        access="unverified",
                        metadata={"gutenberg_id": book_id, "download_count": book.get("download_count")},
                    )
                )
            if not added and config.include_metadata_only:
                results.append(
                    DocumentResult(
                        title=title,
                        source=self.name,
                        authors=authors,
                        language=languages,
                        fmt="metadata",
                        source_url=source_url,
                        access="metadata",
                        metadata={"gutenberg_id": book_id, "formats": list(formats.keys())},
                    )
                )
        return results


class ArxivProvider(SourceProvider):
    name = "arXiv"
    key = "arxiv"
    API_URL = "https://export.arxiv.org/api/query"
    ATOM_NS = "{http://www.w3.org/2005/Atom}"
    ARXIV_NS = "{http://arxiv.org/schemas/atom}"

    def _build_arxiv_query(self, query: str) -> str:
        tokens = re.findall(r"[\w\-]+", clean_query(query).lower(), flags=re.UNICODE)
        tokens = [token for token in tokens if len(token) > 1][:8]
        if not tokens:
            return f'all:"{clean_query(query)}"'
        return " AND ".join(f"all:{token}" for token in tokens)

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        if "pdf" not in config.formats:
            return []
        params = {
            "search_query": self._build_arxiv_query(query),
            "start": 0,
            "max_results": config.limit_per_source,
            "sortBy": "relevance",
            "sortOrder": "descending",
        }
        response = self.session.get(self.API_URL, params=params, timeout=(10, 45))
        response.raise_for_status()
        root = ET.fromstring(response.content)
        results: list[DocumentResult] = []
        for entry in root.findall(f"{self.ATOM_NS}entry"):
            title = normalize_spaces(entry.findtext(f"{self.ATOM_NS}title") or "Sin título")
            source_url = entry.findtext(f"{self.ATOM_NS}id") or ""
            published = entry.findtext(f"{self.ATOM_NS}published") or ""
            year = published[:4] if published else ""
            summary = normalize_spaces(entry.findtext(f"{self.ATOM_NS}summary") or "")
            authors = [normalize_spaces(a.findtext(f"{self.ATOM_NS}name") or "") for a in entry.findall(f"{self.ATOM_NS}author")]
            authors = [a for a in authors if a]
            license_el = entry.find(f"{self.ARXIV_NS}license")
            license_text = license_el.text if license_el is not None and license_el.text else ""

            pdf_url = ""
            for link in entry.findall(f"{self.ATOM_NS}link"):
                if link.attrib.get("title") == "pdf" or link.attrib.get("type") == "application/pdf":
                    pdf_url = link.attrib.get("href", "")
                    break
            if not pdf_url and "/abs/" in source_url:
                pdf_url = source_url.replace("/abs/", "/pdf/")
            pdf_url = pdf_url.replace("http://", "https://", 1)

            results.append(
                DocumentResult(
                    title=title,
                    source=self.name,
                    authors=authors,
                    year=year,
                    fmt="pdf",
                    download_url=pdf_url,
                    source_url=source_url,
                    license=license_text,
                    description=summary[:400],
                    access="unverified" if pdf_url else "metadata",
                    metadata={"published": published},
                )
            )
        return results


class DoabProvider(SourceProvider):
    name = "DOAB"
    key = "doab"
    API_URL = "https://directory.doabooks.org/rest/search"
    BASE_URL = "https://directory.doabooks.org"

    def _items_from_payload(self, payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        for key in ["items", "results", "content", "data"]:
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return []

    def _meta_values(self, item: dict[str, Any], *keys: str) -> list[str]:
        metadata = item.get("metadata") or []
        values: list[str] = []
        if isinstance(metadata, dict):
            for key in keys:
                raw = metadata.get(key) or metadata.get(key.replace(".", "_"))
                for entry in coerce_list(raw):
                    if isinstance(entry, dict):
                        val = entry.get("value") or entry.get("language") or entry.get("display")
                    else:
                        val = entry
                    text = normalize_spaces(val)
                    if text:
                        values.append(text)
        elif isinstance(metadata, list):
            for entry in metadata:
                if not isinstance(entry, dict):
                    continue
                if entry.get("key") in keys or entry.get("element") in keys:
                    text = normalize_spaces(entry.get("value"))
                    if text:
                        values.append(text)
        return values

    def _source_url(self, item: dict[str, Any]) -> str:
        handle = first_text(item.get("handle"), item.get("identifier"))
        if handle:
            if handle.startswith("http"):
                return handle
            return f"{self.BASE_URL}/handle/{handle}"
        link = first_text(item.get("link"), item.get("self"), item.get("url"))
        if link:
            return urljoin(self.BASE_URL, link)
        return "https://www.doabooks.org/"

    def _bitstreams(self, item: dict[str, Any]) -> list[dict[str, Any]]:
        streams = item.get("bitstreams") or item.get("files") or []
        if isinstance(streams, list):
            return [s for s in streams if isinstance(s, dict)]
        return []

    def _bitstream_url(self, bitstream: dict[str, Any]) -> str:
        for key in ["retrieveLink", "downloadUrl", "download_url", "href", "link", "url"]:
            value = bitstream.get(key)
            if value:
                return urljoin(self.BASE_URL, ftp_to_https(str(value)))
        uuid = bitstream.get("uuid") or bitstream.get("id")
        if uuid:
            return f"{self.BASE_URL}/bitstreams/{quote(str(uuid), safe='')}/download"
        return ""

    def _bitstream_fmt(self, bitstream: dict[str, Any], url: str) -> str:
        name = first_text(bitstream.get("name"), bitstream.get("filename"), url).lower()
        mime = first_text(bitstream.get("mimeType"), bitstream.get("mime_type"), bitstream.get("format")).lower()
        if "pdf" in mime or name.endswith(".pdf"):
            return "pdf"
        if "epub" in mime or name.endswith(".epub"):
            return "epub"
        if "text/plain" in mime or name.endswith(".txt"):
            return "txt"
        if "html" in mime or name.endswith((".html", ".htm")):
            return "html"
        if name.endswith(".docx"):
            return "docx"
        if name.endswith(".doc"):
            return "doc"
        return extension_from_url(url)

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        params = {
            "query": clean_query(query),
            "expand": "metadata,bitstreams",
            "limit": config.limit_per_source,
        }
        payload = request_json(
            self.session,
            self.API_URL,
            params=params,
            headers={"Accept": "application/json", "User-Agent": USER_AGENT},
        )
        items = self._items_from_payload(payload)
        results: list[DocumentResult] = []
        for item in items:
            title = first_text(item.get("name"), self._meta_values(item, "dc.title", "title")) or "Sin título"
            authors = self._meta_values(item, "dc.contributor.author", "dc.creator", "creator", "author")
            year = first_text(self._meta_values(item, "dc.date.issued", "dc.date", "date"))[:4]
            language = first_text(self._meta_values(item, "dc.language.iso", "dc.language", "language"))
            license_text = first_text(self._meta_values(item, "dc.rights.uri", "dc.rights", "license"))
            description = first_text(self._meta_values(item, "dc.description.abstract", "dc.description", "description"))[:400]
            source_url = self._source_url(item)
            added = False
            for bitstream in self._bitstreams(item):
                url = self._bitstream_url(bitstream)
                if not url:
                    continue
                fmt = self._bitstream_fmt(bitstream, url)
                if fmt not in config.formats:
                    continue
                added = True
                results.append(
                    DocumentResult(
                        title=title,
                        source=self.name,
                        authors=authors,
                        year=year,
                        language=language,
                        fmt=fmt,
                        download_url=url,
                        source_url=source_url,
                        license=license_text,
                        description=description,
                        access="unverified",
                        metadata={"handle": item.get("handle"), "bitstream": bitstream},
                    )
                )
            if not added and config.include_metadata_only:
                results.append(
                    DocumentResult(
                        title=title,
                        source=self.name,
                        authors=authors,
                        year=year,
                        language=language,
                        fmt="metadata",
                        source_url=source_url,
                        license=license_text,
                        description=description or "Resultado de DOAB sin bitstream en los formatos configurados.",
                        access="metadata",
                        metadata={"handle": item.get("handle")},
                    )
                )
        return results


class EuropePmcProvider(SourceProvider):
    name = "Europe PMC"
    key = "europe_pmc"
    API_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
    PMC_OA_URL = "https://www.ncbi.nlm.nih.gov/pmc/utils/oa/oa.fcgi"

    def _author_list(self, rec: dict[str, Any]) -> list[str]:
        author_string = normalize_spaces(rec.get("authorString"))
        if author_string:
            return [a.strip() for a in re.split(r",\s+|;\s+", author_string) if a.strip()][:8]
        authors = rec.get("authorList", {}).get("author", []) if isinstance(rec.get("authorList"), dict) else []
        out = []
        for author in coerce_list(authors):
            if isinstance(author, dict):
                out.append(first_text(author.get("fullName"), author.get("lastName")))
        return [a for a in out if a][:8]

    def _source_url(self, rec: dict[str, Any]) -> str:
        pmcid = normalize_spaces(rec.get("pmcid"))
        if pmcid:
            return f"https://europepmc.org/articles/{pmcid}"
        source = normalize_spaces(rec.get("source"))
        article_id = normalize_spaces(rec.get("id"))
        if source and article_id:
            return f"https://europepmc.org/article/{quote(source)}/{quote(article_id)}"
        doi = normalize_spaces(rec.get("doi"))
        if doi:
            return f"https://doi.org/{doi}"
        return "https://europepmc.org/"

    def _fulltext_pdf_url(self, rec: dict[str, Any]) -> str:
        ft_list = rec.get("fullTextUrlList", {})
        urls = []
        if isinstance(ft_list, dict):
            urls = coerce_list(ft_list.get("fullTextUrl"))
        elif isinstance(ft_list, list):
            urls = ft_list
        for item in urls:
            if not isinstance(item, dict):
                continue
            url = normalize_spaces(item.get("url"))
            style = normalize_spaces(item.get("documentStyle")).lower()
            availability = normalize_spaces(item.get("availability")).lower()
            if url and (style == "pdf" or ".pdf" in url.lower() or "pdf=render" in url.lower()):
                if not availability or availability in {"open access", "free", "free full text"}:
                    return ftp_to_https(url)
        return ""

    def _pmc_oa_pdf_url(self, pmcid: str) -> tuple[str, str]:
        if not pmcid:
            return "", ""
        try:
            response = self.session.get(self.PMC_OA_URL, params={"id": pmcid}, timeout=(10, 30))
            response.raise_for_status()
            root = ET.fromstring(response.content)
        except Exception:
            return "", ""
        for record in root.findall(".//record"):
            license_text = record.attrib.get("license", "")
            for link in record.findall("link"):
                if link.attrib.get("format") == "pdf" and link.attrib.get("href"):
                    return ftp_to_https(link.attrib["href"]), license_text
        return "", ""

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        if "pdf" not in config.formats:
            return []
        epmc_query = f"({clean_query(query)}) AND OPEN_ACCESS:Y"
        params = {
            "query": epmc_query,
            "format": "json",
            "resultType": "core",
            "pageSize": config.limit_per_source,
            "sort": "RELEVANCE",
        }
        payload = request_json(self.session, self.API_URL, params=params)
        assert isinstance(payload, dict)
        records = payload.get("resultList", {}).get("result", [])
        results: list[DocumentResult] = []
        for rec in records:
            title = normalize_spaces(rec.get("title") or "Sin título")
            pmcid = normalize_spaces(rec.get("pmcid"))
            pdf_url = self._fulltext_pdf_url(rec)
            pmc_license = ""
            if not pdf_url and pmcid and normalize_spaces(rec.get("hasPDF")).upper() == "Y":
                pdf_url, pmc_license = self._pmc_oa_pdf_url(pmcid)
            if not pdf_url and pmcid:
                # Último recurso: endpoint renderizado público; la validación descartará si falla.
                pdf_url = f"https://europepmc.org/articles/{pmcid}?pdf=render"
            source_url = self._source_url(rec)
            results.append(
                DocumentResult(
                    title=title,
                    source=self.name,
                    authors=self._author_list(rec),
                    year=normalize_spaces(rec.get("pubYear")),
                    language="",
                    fmt="pdf" if pdf_url else "metadata",
                    download_url=pdf_url,
                    source_url=source_url,
                    license=pmc_license or normalize_spaces(rec.get("license")),
                    description=normalize_spaces(rec.get("abstractText"))[:400],
                    access="unverified" if pdf_url else "metadata",
                    metadata={"pmcid": pmcid, "doi": rec.get("doi"), "journal": rec.get("journalTitle")},
                )
            )
        return results


class OpenLibraryProvider(SourceProvider):
    name = "Open Library"
    key = "open_library"
    API_URL = "https://openlibrary.org/search.json"

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        params = {
            "q": clean_query(query),
            "fields": "title,author_name,first_publish_year,key,ia,public_scan_b,has_fulltext,edition_count,language",
            "limit": config.limit_per_source,
        }
        if config.language:
            params["lang"] = config.language.lower()
        payload = request_json(self.session, self.API_URL, params=params)
        assert isinstance(payload, dict)
        docs = payload.get("docs", [])
        results: list[DocumentResult] = []
        for doc in docs:
            key = doc.get("key") or ""
            source_url = f"https://openlibrary.org{key}" if key else "https://openlibrary.org/"
            ia_ids = doc.get("ia") or []
            if doc.get("public_scan_b") and ia_ids:
                description = f"Tiene escaneo público asociado en Internet Archive: {ia_ids[0]}"
            elif doc.get("has_fulltext"):
                description = "Tiene texto completo/disponibilidad reportada por Open Library. Abre la ficha para revisar préstamo o lectura."
            else:
                description = "Resultado de catálogo; puede requerir préstamo o consulta externa."
            results.append(
                DocumentResult(
                    title=normalize_spaces(doc.get("title") or "Sin título"),
                    source=self.name,
                    authors=doc.get("author_name") or [],
                    year=str(doc.get("first_publish_year") or ""),
                    language=", ".join((doc.get("language") or [])[:3]),
                    fmt="metadata",
                    source_url=source_url,
                    description=description,
                    access="metadata",
                    metadata={"openlibrary_key": key, "ia": ia_ids, "edition_count": doc.get("edition_count")},
                )
            )
        return results


class OpenAlexProvider(SourceProvider):
    name = "OpenAlex"
    key = "openalex"
    API_URL = "https://api.openalex.org/works"

    def _author_list(self, work: dict[str, Any]) -> list[str]:
        authors = []
        for authorship in work.get("authorships") or []:
            if not isinstance(authorship, dict):
                continue
            author = authorship.get("author") or {}
            name = normalize_spaces(author.get("display_name"))
            if name:
                authors.append(name)
        return authors[:8]

    def _candidate_pdf_url(self, work: dict[str, Any]) -> str:
        locations = []
        for key in ["best_oa_location", "primary_location"]:
            if isinstance(work.get(key), dict):
                locations.append(work[key])
        locations.extend([loc for loc in (work.get("locations") or []) if isinstance(loc, dict)])
        for loc in locations:
            if loc.get("is_oa") and loc.get("pdf_url"):
                return ftp_to_https(str(loc["pdf_url"]))
        has_content = work.get("has_content") or {}
        if isinstance(has_content, dict) and has_content.get("pdf") and has_content.get("content_url"):
            return ftp_to_https(str(has_content["content_url"]))
        return ""

    def _landing_url(self, work: dict[str, Any]) -> str:
        for key in ["best_oa_location", "primary_location"]:
            loc = work.get(key)
            if isinstance(loc, dict) and loc.get("landing_page_url"):
                return str(loc["landing_page_url"])
        return normalize_spaces(work.get("doi")) or normalize_spaces(work.get("id")) or "https://openalex.org/"

    def search(self, query: str, config: SearchConfig) -> list[DocumentResult]:
        api_key = os.getenv("OPENALEX_API_KEY", "").strip()
        if not api_key:
            console.print("[yellow]OpenAlex omitido: define OPENALEX_API_KEY para usar esta fuente.[/yellow]")
            return []
        if "pdf" not in config.formats:
            return []
        params = {
            "api_key": api_key,
            "search": clean_query(query),
            "filter": "open_access.is_oa:true",
            "per_page": min(config.limit_per_source, 25),
            "select": "id,doi,display_name,publication_year,language,type,open_access,best_oa_location,primary_location,locations,authorships,has_content",
        }
        payload = request_json(self.session, self.API_URL, params=params)
        assert isinstance(payload, dict)
        results = []
        for work in payload.get("results", []):
            pdf_url = self._candidate_pdf_url(work)
            source_url = self._landing_url(work)
            oa = work.get("open_access") or {}
            results.append(
                DocumentResult(
                    title=normalize_spaces(work.get("display_name") or "Sin título"),
                    source=self.name,
                    authors=self._author_list(work),
                    year=str(work.get("publication_year") or ""),
                    language=normalize_spaces(work.get("language")),
                    fmt="pdf" if pdf_url else "metadata",
                    download_url=pdf_url,
                    source_url=source_url,
                    license=normalize_spaces((work.get("best_oa_location") or {}).get("license") or oa.get("oa_status")),
                    description=normalize_spaces(work.get("type")),
                    access="unverified" if pdf_url else "metadata",
                    metadata={"openalex_id": work.get("id"), "doi": work.get("doi")},
                )
            )
        return results


PROVIDER_CLASSES = {
    InternetArchiveProvider.key: InternetArchiveProvider,
    GutenbergProvider.key: GutenbergProvider,
    ArxivProvider.key: ArxivProvider,
    DoabProvider.key: DoabProvider,
    EuropePmcProvider.key: EuropePmcProvider,
    OpenLibraryProvider.key: OpenLibraryProvider,
    OpenAlexProvider.key: OpenAlexProvider,
}


# ---------------------------------------------------------------------------
# Descarga y exportación
# ---------------------------------------------------------------------------


def build_output_path(result: DocumentResult, config: SearchConfig) -> Path:
    ext = result.fmt if result.fmt in ALL_FORMATS else extension_from_url(result.download_url)
    if ext == "bin":
        ext = extension_from_url(result.download_url)
    if ext == "bin":
        ext = result.fmt or "bin"
    source_dir = safe_filename(result.source)
    year_dir = safe_filename(result.year or "sin_anio")
    title = safe_filename(result.title)
    token = short_hash(result.download_url or result.source_url or title)
    folder = config.download_dir / source_dir / year_dir
    folder.mkdir(parents=True, exist_ok=True)
    return ensure_unique_path(folder / f"{title}__{token}.{ext}")


def save_metadata(result: DocumentResult, file_path: Path) -> None:
    metadata_path = file_path.with_suffix(file_path.suffix + ".metadata.json")
    data = asdict(result)
    data["downloaded_file"] = str(file_path)
    metadata_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def check_response_size(response: Response, config: SearchConfig, title: str) -> Optional[int]:
    length_raw = response.headers.get("content-length")
    total = parse_content_length(length_raw)
    if total is None:
        return None
    max_bytes = config.max_download_mb * 1024 * 1024
    if total > max_bytes:
        raise BookyError(
            f"'{title}' pesa aproximadamente {total / 1024 / 1024:.1f} MB, "
            f"supera el límite configurado de {config.max_download_mb} MB."
        )
    return total


def download_result(session: Session, result: DocumentResult, config: SearchConfig) -> Optional[Path]:
    if not result.downloadable:
        console.print(f"[yellow]'{result.title}' no tiene descarga directa pública verificada. Abriendo ficha…[/yellow]")
        if result.source_url:
            webbrowser.open(result.source_url)
        return None

    # Revalidación ligera justo antes de descargar; útil si el enlace caducó entre búsqueda y descarga.
    probe = probe_download_url(session, result.download_url, result.fmt, config)
    if not probe.ok:
        result.metadata["last_probe"] = asdict(probe)
        result.download_url = ""
        result.access = "restricted" if probe.status_code in {401, 403} else "dead"
        console.print(f"[yellow]La descarga ya no está disponible: {probe.reason}. Abriendo ficha…[/yellow]")
        if result.source_url:
            webbrowser.open(result.source_url)
        return None
    result.download_url = probe.final_url or result.download_url

    output_path = build_output_path(result, config)
    temp_path = output_path.with_suffix(output_path.suffix + ".part")
    console.print(f"\n[bold]Descargando:[/bold] {result.title} [dim]({result.source}, {result.fmt})[/dim]")

    try:
        with session.get(result.download_url, stream=True, timeout=(10, 120), allow_redirects=True) as response:
            response.raise_for_status()
            if not content_type_matches(result.fmt, response.url, response.headers.get("content-type", "")):
                raise BookyError(f"El servidor devolvió tipo inesperado: {response.headers.get('content-type', '')}")
            total = check_response_size(response, config, result.title)
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task_id = progress.add_task("descarga", total=total)
                with temp_path.open("wb") as file:
                    for chunk in response.iter_content(chunk_size=1024 * 128):
                        if not chunk:
                            continue
                        file.write(chunk)
                        progress.update(task_id, advance=len(chunk))
        temp_path.rename(output_path)
        save_metadata(result, output_path)
        console.print(f"[green]Guardado en:[/green] {output_path}")
        return output_path
    except Exception:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)
        raise


def export_results(results: list[DocumentResult], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    json_path = output_dir / f"booky_resultados_{stamp}.json"
    csv_path = output_dir / f"booky_resultados_{stamp}.csv"
    json_path.write_text(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2), encoding="utf-8")
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "title",
                "source",
                "authors",
                "year",
                "language",
                "fmt",
                "access",
                "download_url",
                "source_url",
                "license",
                "description",
            ],
        )
        writer.writeheader()
        for r in results:
            row = asdict(r)
            row["authors"] = "; ".join(r.authors)
            row.pop("metadata", None)
            writer.writerow(row)
    console.print(f"[green]Exportado:[/green] {json_path}")
    console.print(f"[green]Exportado:[/green] {csv_path}")


# ---------------------------------------------------------------------------
# UI / Menú
# ---------------------------------------------------------------------------


def render_header(config: SearchConfig) -> None:
    subtitle = (
        f"Fuentes: {', '.join(config.sources)}\n"
        f"Formatos: {', '.join(config.formats)} | Idioma: {config.language or 'sin filtro'} | "
        f"Límite/fuente: {config.limit_per_source}\n"
        f"Verificar enlaces: {'sí' if config.verify_links else 'no'} | "
        f"Fichas sin descarga: {'sí' if config.include_metadata_only else 'no'}\n"
        f"Carpeta: {config.download_dir} | Máx. descarga: {config.max_download_mb} MB"
    )
    console.print(Panel.fit(f"[bold cyan]{APP_NAME}[/bold cyan]\n{subtitle}", box=box.ROUNDED))


def render_results(results: list[DocumentResult]) -> None:
    if not results:
        console.print("[yellow]No hubo resultados.[/yellow]")
        return
    table = Table(title="Resultados", box=box.SIMPLE_HEAVY)
    table.add_column("#", justify="right", style="bold")
    table.add_column("Título", overflow="fold", ratio=3)
    table.add_column("Fuente")
    table.add_column("Formato")
    table.add_column("Acceso", justify="center")
    table.add_column("Año", justify="center")
    table.add_column("Autor(es)", overflow="fold", ratio=2)
    for idx, r in enumerate(results, start=1):
        table.add_row(
            str(idx),
            r.title,
            r.source,
            r.fmt,
            r.access_label,
            r.year or "—",
            r.short_authors,
        )
    console.print(table)
    console.print(
        "[dim]✅ directo = descargable validado | 🔎 ficha = abrir página | ⛔ restringido = requiere préstamo/login/permisos | ❌ caído = enlace descartado[/dim]"
    )


def build_safe_dorks(query: str, formats: list[str]) -> list[tuple[str, str, str]]:
    # Consultas avanzadas limitadas a dominios abiertos/educativos. No usan patrones de evasión.
    fmt_filetypes = " OR ".join(f"filetype:{fmt}" for fmt in formats if fmt in {"pdf", "txt", "doc", "docx", "epub"})
    if not fmt_filetypes:
        fmt_filetypes = "filetype:pdf"
    phrase = f'"{clean_query(query)}"'
    dorks = [
        ("Project Gutenberg", f"site:gutenberg.org {phrase} (EPUB OR TXT OR HTML)", "https://www.google.com/search?q="),
        ("DOAB", f"site:doabooks.org OR site:directory.doabooks.org {phrase} (PDF OR EPUB)", "https://www.google.com/search?q="),
        ("OAPEN", f"site:oapen.org {phrase} (PDF OR EPUB)", "https://www.google.com/search?q="),
        ("arXiv", f"site:arxiv.org/pdf {phrase}", "https://www.google.com/search?q="),
        ("Europe PMC", f"site:europepmc.org/articles {phrase} PDF", "https://www.google.com/search?q="),
        ("OpenStax", f"site:openstax.org {fmt_filetypes} {phrase}", "https://www.google.com/search?q="),
        ("Wikisource", f"site:wikisource.org {phrase}", "https://www.google.com/search?q="),
        ("Repositorios universitarios", f"site:.edu {fmt_filetypes} {phrase} (\"open access\" OR repository)", "https://www.google.com/search?q="),
    ]
    return [(name, dork, prefix + quote_plus(dork)) for name, dork, prefix in dorks]


def render_dorks(query: str, formats: list[str]) -> None:
    table = Table(title="Consultas avanzadas en dominios abiertos", box=box.SIMPLE_HEAVY)
    table.add_column("Fuente")
    table.add_column("Consulta", overflow="fold", ratio=4)
    dorks = build_safe_dorks(query, formats)
    for name, dork, _url in dorks:
        table.add_row(name, dork)
    console.print(table)
    console.print(
        "[dim]No incluye intitle:index.of, backups, credenciales, directorios expuestos ni patrones para saltarse permisos. Sirve para abrir búsquedas en repositorios legítimos.[/dim]"
    )
    if Confirm.ask("¿Abrir todas estas búsquedas en el navegador?", default=False):
        for _name, _dork, url in dorks:
            webbrowser.open(url)


class BookyApp:
    def __init__(self) -> None:
        self.config = SearchConfig()
        self.session = create_session()
        self.last_results: list[DocumentResult] = []

    def provider_instances(self) -> list[SourceProvider]:
        providers: list[SourceProvider] = []
        for key in self.config.sources:
            cls = PROVIDER_CLASSES.get(key)
            if cls:
                providers.append(cls(self.session))
        return providers

    def run_search(self) -> None:
        query = Prompt.ask("[bold]¿Qué documento/libro/paper quieres buscar?[/bold]").strip()
        if not query:
            return
        limit = IntPrompt.ask("Resultados por fuente", default=self.config.limit_per_source)
        self.config.limit_per_source = max(1, min(limit, 50))

        all_results: list[DocumentResult] = []
        for provider in self.provider_instances():
            with console.status(f"Buscando en {provider.name}…"):
                try:
                    found = provider.search(query, self.config)
                    all_results.extend(found)
                except requests.HTTPError as exc:
                    console.print(f"[red]Error HTTP en {provider.name}:[/red] {exc}")
                except requests.RequestException as exc:
                    console.print(f"[red]Error de red en {provider.name}:[/red] {exc}")
                except Exception as exc:  # noqa: BLE001
                    console.print(f"[red]Error en {provider.name}:[/red] {exc}")

        unique = dedupe_results(all_results)
        self.last_results = validate_direct_links(self.session, unique, self.config)
        if not self.config.include_metadata_only:
            self.last_results = [r for r in self.last_results if r.downloadable]
        render_results(self.last_results)
        self.results_actions()

    def results_actions(self) -> None:
        if not self.last_results:
            return
        while True:
            action = Prompt.ask(
                "Acción: números '1,3,5' o rango '1-4' para descargar/abrir, [bold]todos[/bold], [bold]abrir[/bold], [bold]exportar[/bold], [bold]menu[/bold]",
                default="menu",
            ).strip().lower()
            if action in {"menu", "m", "volver"}:
                return
            if action in {"exportar", "e"}:
                export_results(self.last_results, self.config.download_dir)
                continue
            if action in {"abrir", "a"}:
                idx = IntPrompt.ask("Número del resultado a abrir", default=1)
                if 1 <= idx <= len(self.last_results):
                    url = self.last_results[idx - 1].source_url or self.last_results[idx - 1].download_url
                    if url:
                        webbrowser.open(url)
                continue
            if action in {"todos", "todo", "all"}:
                selected = [i + 1 for i, r in enumerate(self.last_results) if r.downloadable]
                if not selected:
                    console.print("[yellow]No hay descargas directas verificadas en la lista.[/yellow]")
                    continue
            else:
                try:
                    selected = parse_indices(action, len(self.last_results))
                except Exception as exc:  # noqa: BLE001
                    console.print(f"[yellow]Selección inválida:[/yellow] {exc}")
                    continue

            for idx in selected:
                result = self.last_results[idx - 1]
                try:
                    download_result(self.session, result, self.config)
                    time.sleep(0.75)
                except Exception as exc:  # noqa: BLE001
                    console.print(f"[red]No pude descargar/abrir '{result.title}':[/red] {exc}")

    def configure(self) -> None:
        console.print("\n[bold]Configuración actual[/bold]")
        render_header(self.config)
        try:
            formats = Prompt.ask("Formatos separados por coma", default=", ".join(self.config.formats))
            self.config.formats = parse_format_list(formats)
            sources = Prompt.ask(
                "Fuentes: gutenberg, arxiv, doab, europepmc, ia, openlibrary, openalex",
                default=", ".join(self.config.sources),
            )
            self.config.sources = parse_source_list(sources)
            language = Prompt.ask("Idioma ISO-639-1, ej. es/en/fr, vacío para no filtrar", default=self.config.language)
            self.config.language = language.strip().lower()[:2]
            limit = IntPrompt.ask("Resultados por fuente", default=self.config.limit_per_source)
            self.config.limit_per_source = max(1, min(limit, 50))
            max_mb = IntPrompt.ask("Tamaño máximo por descarga en MB", default=self.config.max_download_mb)
            self.config.max_download_mb = max(1, max_mb)
            self.config.verify_links = Confirm.ask("Validar enlaces antes de mostrarlos como descargables", default=self.config.verify_links)
            self.config.include_metadata_only = Confirm.ask(
                "Mostrar también fichas sin descarga directa", default=self.config.include_metadata_only
            )
            folder = Prompt.ask("Carpeta de descargas", default=str(self.config.download_dir))
            self.config.download_dir = Path(folder).expanduser()
        except BookyError as exc:
            console.print(f"[red]{exc}[/red]")

    def dork_menu(self) -> None:
        query = Prompt.ask("Consulta para generar búsquedas avanzadas en dominios abiertos").strip()
        if query:
            render_dorks(query, self.config.formats)

    def direct_url_download(self) -> None:
        console.print("[yellow]Usa esto solo con URLs directas que tengas derecho de descargar.[/yellow]")
        url = Prompt.ask("URL directa").strip()
        if not url.lower().startswith(("http://", "https://", "ftp://")):
            console.print("[red]La URL debe empezar por http://, https:// o ftp://[/red]")
            return
        url = ftp_to_https(url)
        title = Prompt.ask("Título/nombre para guardar", default=Path(urlparse(url).path).name or "documento")
        fmt = extension_from_url(url)
        if fmt == "bin" or fmt not in ALL_FORMATS:
            fmt = Prompt.ask("No pude detectar formato. Escribe extensión", default="pdf").strip().lower().lstrip(".")
        result = DocumentResult(title=title, source="URL directa", fmt=fmt, download_url=url, source_url=url, access="unverified")
        probe = probe_download_url(self.session, url, fmt, self.config)
        result.metadata["url_probe"] = asdict(probe)
        if probe.ok:
            result.access = "verified"
            result.download_url = probe.final_url or url
        else:
            console.print(f"[red]La URL no parece descargable:[/red] {probe.reason}")
            return
        try:
            download_result(self.session, result, self.config)
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]No pude descargar la URL:[/red] {exc}")

    def main_menu(self) -> None:
        while True:
            console.clear()
            render_header(self.config)
            table = Table(show_header=False, box=box.ROUNDED)
            table.add_column("Opción", style="bold cyan", justify="right")
            table.add_column("Acción")
            table.add_row("1", "Buscar documentos abiertos")
            table.add_row("2", "Configurar fuentes, formatos, idioma y carpeta")
            table.add_row("3", "Generar consultas avanzadas en dominios abiertos")
            table.add_row("4", "Descargar URL directa autorizada")
            table.add_row("5", "Exportar últimos resultados")
            table.add_row("6", "Salir")
            console.print(table)
            choice = Prompt.ask("Elige", choices=["1", "2", "3", "4", "5", "6"], default="1")
            if choice == "1":
                self.run_search()
                Prompt.ask("Presiona Enter para volver al menú", default="")
            elif choice == "2":
                self.configure()
                Prompt.ask("Presiona Enter para volver al menú", default="")
            elif choice == "3":
                self.dork_menu()
                Prompt.ask("Presiona Enter para volver al menú", default="")
            elif choice == "4":
                self.direct_url_download()
                Prompt.ask("Presiona Enter para volver al menú", default="")
            elif choice == "5":
                if self.last_results:
                    export_results(self.last_results, self.config.download_dir)
                else:
                    console.print("[yellow]Todavía no hay resultados para exportar.[/yellow]")
                Prompt.ask("Presiona Enter para volver al menú", default="")
            elif choice == "6":
                console.print("[bold green]¡Gracias por usar Booky Open![/bold green]")
                return


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------


def main() -> int:
    try:
        app = BookyApp()
        app.main_menu()
        return 0
    except KeyboardInterrupt:
        console.print("\n[yellow]Salida solicitada por el usuario.[/yellow]")
        return 130


if __name__ == "__main__":
    sys.exit(main())
