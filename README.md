# Descarga Libros Gratis En Línea Con Booky

El script permite que el usuario ingrese el título del libro que desea buscar, especifica cuántos resultados desea obtener y puede seleccionar uno o varios libros para descargar. La entrada del usuario se valida y se procesa en un bucle while. El usuario tiene varias opciones, como ingresar "salir" para salir del programa, "buscar" para realizar una nueva búsqueda, "descargar" para descargar todos los libros o ingresar números separados por comas para descargar libros específicos. Cuando se realiza una búsqueda, el script utiliza la biblioteca googlesearch para buscar libros relacionados con la consulta y obtener los resultados en formato PDF. Los resultados de la búsqueda se muestran al usuario, y se le permite elegir qué libros desea descargar o si desea buscar más libros.

<img align="center" height="480" width="1000" alt="GIF" src="https://github.com/Yextep/Booky/assets/114537444/6cfae5c9-5392-49b7-ae0b-4ba61f1d4029"/>

# Características principales
- 📗 **Búsqueda personalizada:** Ingresa "libro para aprender yoga" o "libro de programación"  y obtén resultados específicos.
- ✏️ **Flexibilidad:** Descarga un libro, varios o todos los encontrados.
- 📲 **Descargas organizadas:** Los libros descargados se almacenan en una carpeta designada.
- ✏️ **Gestión de errores:** El script maneja entradas incorrectas y permite volver a intentarlo.
- 📗 **Reconocimiento de tíldes:** Soporta tíldes y caracteres especiales para una búsqueda precisa.

# Instalación

Clonamos el repositorio
```bash
git clone https://github.com/Yextep/Booky
```
Accedemos a la carpeta
```bash
cd Booky
```
Instalamos requerimientos
```bash
pip install -r requeriments.txt
```
Ejecutamos el Script
```bash
python3 booky.py
```
