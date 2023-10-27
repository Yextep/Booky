import os
from googlesearch import search
import requests
from bs4 import BeautifulSoup
import urllib.parse

# Función para buscar libros en línea
def buscar_libros():
    while True:
        try:
            query = input("Ingresa 'libro' antes de lo que deseas buscar (ejemplo: 'libro de hacking' o 'salir' para salir): ")
            if query.lower() == "salir":
                break

            num_resultados = int(input("Cuántos resultados quieres obtener? (ejemplo '15'): "))

            query = urllib.parse.quote_plus(query)  # Codifica caracteres especiales
            results = list(search(query + " filetype:pdf", num_results=num_resultados, lang="es"))

            print("Resultados de la búsqueda:")
            for i, result in enumerate(results):
                print(f"{i+1}. {obtener_nombre_libro(result)}")

            seleccion = input("Ingresa el numero del libro que quieras descargar, si quieres descargar varios ingresa los numeros separados por comas, si quieres descargar todos 'descargar', si quieres buscar más libros 'buscar', si quieres salir 'salir'): ").strip().lower()
            if seleccion == "salir":
                break
            elif seleccion == "buscar":
                continue
            elif seleccion == "descargar":
                descargar_todos(resultados=results)
            else:
                seleccion = [int(num) for num in seleccion.split(",")]
                for index in seleccion:
                    if 1 <= index <= len(results):
                        descargar_libro(results[index - 1])
                    else:
                        print(f"Selección inválida para el resultado {index}.")

        except (ValueError, IndexError):
            print("Ingresaste algo erróneo. Por favor, intenta de nuevo.")

    print("¡Gracias por usar el descargador de libros en línea!")

# Función para obtener el nombre del libro desde la URL
def obtener_nombre_libro(url):
    titulo = os.path.basename(url)
    titulo = urllib.parse.unquote(titulo)  # Decodifica caracteres especiales
    titulo = titulo.replace(".pdf", "")
    return titulo

# Función para descargar un libro a partir de una URL
def descargar_libro(url):
    try:
        # Obtener el título del libro desde el enlace
        titulo = obtener_nombre_libro(url)
        
        # Crear una carpeta para almacenar el libro
        carpeta_destino = "Libros_Descargados"
        if not os.path.exists(carpeta_destino):
            os.mkdir(carpeta_destino)

        # Descargar el libro
        response = requests.get(url)
        with open(os.path.join(carpeta_destino, f"{titulo}.pdf"), 'wb') as file:
            file.write(response.content)
        print(f"Libro descargado: {titulo}")
    except Exception as e:
        print("Ocurrió un error al descargar el libro:", str(e))

# Función para descargar todos los libros
def descargar_todos(resultados):
    for i, result in enumerate(resultados):
        descargar_libro(result)

if __name__ == "__main__":
    print("¡Bienvenido al descargador de libros en línea!")
    buscar_libros()
