# Descarga Libros Gratis En Línea Con Booky

<img align="center" height="480" width="1000" alt="GIF" src="https://github.com/Yextep/Booky/assets/114537444/6cfae5c9-5392-49b7-ae0b-4ba61f1d4029"/>

Buscador/descargador interactivo de documentos abiertos. Esta versión reemplaza el scraping de Google por APIs públicas y añade validación de enlaces para evitar errores como `401 Unauthorized` en archivos que aparecen en metadatos pero no son descargables públicamente.

## Instalación

```bash
python -m venv .venv
source .venv/bin/activate    # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python booky_open.py
```                                                                    
## Fuentes incluidas                                                   
- **Project Gutenberg / Gutendex**: libros de dominio público.
- **arXiv**: papers/preprints de acceso abierto en PDF.
- **DOAB**: libros académicos open access con bitstreams oficiales.
- **Europe PMC + PMC OA service**: artículos biomédicos open access con PDF cuando existe.
- **Internet Archive**: sólo marca como descargables los archivos que pasan validación; los ítems de préstamo, accesibilidad o bloqueados quedan como ficha.
- **Open Library**: catálogo/metadatos; abre la ficha, no fuerza descargas.
- **OpenAlex**: opcional. Define `OPENALEX_API_KEY` si quieres usarlo:

```bash
export OPENALEX_API_KEY="tu_api_key"
```

## Qué cambió frente a la versión anterior

1. **Validación previa de URLs**: usa `HEAD` y después `GET` con `Range: bytes=0-0` para comprobar si el archivo es realmente accesible sin descargarlo entero.
2. **Internet Archive más seguro**: si un item está restringido, en préstamo o devuelve 401/403, Booky no lo muestra como descarga directa.
3. **Estados claros**:
   - `✅ directo`: descargable validado.
   - `🔎 ficha`: abre la página pública.
   - `⛔ restringido`: requiere permisos, login, préstamo o accesibilidad.
   - `❌ caído`: URL descartada por error o tipo inesperado.
4. **Nuevas fuentes**: DOAB y Europe PMC/PMC OA.
5. **Exportación**: JSON y CSV incluyen `access`, `download_url`, `source_url`, licencia y descripción.

## Uso recomendado

- Para literatura clásica: activa `gutenberg` y formatos `epub,txt,html`.
- Para papers técnicos: activa `arxiv,europepmc,openalex` y formato `pdf`.
- Para libros académicos abiertos: activa `doab` y `pdf,epub`.
- Para catálogos y fichas: activa `openlibrary,internet_archive`.

## Nota legal

Booky Open 2 está diseñado para descubrir y descargar documentos abiertos, de dominio público, open access o descargables con autorización. No incluye técnicas para saltarse pagos, préstamos, credenciales, controles de acceso, directorios expuestos accidentalmente ni material pirateado.
