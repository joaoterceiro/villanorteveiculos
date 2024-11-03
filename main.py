import os
import requests
import xmltodict
from zipfile import ZipFile
from supabase import create_client, Client
import uuid
from datetime import datetime
import logging
import sys
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configuração do logger
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format='%(asctime)s [%(levelname)s] %(message)s')

# Configuração do Supabase
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if not SUPABASE_URL or not SUPABASE_KEY:
    logging.error("As variáveis de ambiente SUPABASE_URL e SUPABASE_KEY não estão definidas.")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Diretório para salvar imagens temporárias
DOWNLOAD_FOLDER = "vehicle_images"
BUCKET_NAME = "vehicle_images"

# URL do XML
XML_URL = "https://app.revendamais.com.br/application/index.php/apiGeneratorXml/generator/sitedaloja/3b46d3e61841f5d1bab0d3b58624b50e7112.xml"

# Função para baixar uma imagem a partir de uma URL
def download_image(url, path):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    except requests.RequestException as e:
        logging.error(f"Erro ao baixar a imagem: {url} - {e}")

# Função para verificar se o arquivo ZIP já existe no bucket
def zip_exists_in_bucket(zip_filename):
    try:
        response = supabase.storage.from_(BUCKET_NAME).list()
        files = response
        exists = any(obj['name'] == zip_filename for obj in files)
        return exists
    except Exception as e:
        logging.error(f"Erro ao verificar o ZIP no bucket: {zip_filename} - {e}")
        return False

# Função para fazer upload do arquivo ZIP para o bucket e obter o link de download
def upload_zip_to_bucket(zip_path, zip_filename):
    try:
        # Verificar se o arquivo já existe no bucket
        if zip_exists_in_bucket(zip_filename):
            # Remover o arquivo existente
            supabase.storage.from_(BUCKET_NAME).remove([zip_filename])
            logging.info(f"Arquivo ZIP existente '{zip_filename}' removido do bucket antes do upload.")
        
        with open(zip_path, "rb") as file:
            supabase.storage.from_(BUCKET_NAME).upload(zip_filename, file)
        
        # Obter o link público usando get_public_url
        download_url = supabase.storage.from_(BUCKET_NAME).get_public_url(zip_filename)
        return download_url
    except Exception as e:
        logging.error(f"Erro ao fazer upload do ZIP: {zip_filename} - {e}")
        return None

# Função para obter dados XML
def fetch_xml_data(url):
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return xmltodict.parse(response.content)
    except requests.RequestException as e:
        logging.error(f"Erro ao acessar o XML: {e}")
        return None

# Função para converter valores de forma segura
def safe_int(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_float(value, default=0.0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# Função para verificar e atualizar dados no Supabase a partir do XML
def update_supabase_from_xml(data):
    xml_external_ids = set()
    for item in data.get("ADS", {}).get("AD", []):
        # Extrair dados do item
        vehicle_id = str(uuid.uuid4())
        external_id = safe_int(item.get("ID"))
        xml_external_ids.add(external_id)
        title = item.get("TITLE", "")
        category = item.get("CATEGORY", "")
        description = item.get("DESCRIPTION", "")
        make = item.get("MAKE", "")
        model = item.get("MODEL", "")
        base_model = item.get("BASE_MODEL", "")
        year = safe_int(item.get("YEAR"))
        manufacture_year = safe_int(item.get("FABRIC_YEAR"))
        condition = item.get("CONDITION", "")
        mileage = safe_int(item.get("MILEAGE"))
        fuel_type = item.get("FUEL", "")
        transmission = item.get("GEAR", "")
        engine = item.get("MOTOR", "")
        plate = item.get("PLATE", "")
        chassis = item.get("CHASSI", "")
        doors = safe_int(item.get("DOORS")) if category.lower() != "motocicleta" else None
        color = item.get("COLOR", "")
        price = safe_float(item.get("PRICE"))
        promotion_price = safe_float(item.get("PROMOTION_PRICE"))
        horsepower = safe_int(item.get("HP"))
        body_type = item.get("BODY_TYPE", "")
        fipe_code = item.get("FIPE", "")
        fipe_value = safe_float(item.get("VALOR_FIPE"))
        
        images = item.get("IMAGES", {}).get("IMAGE_URL", [])
        images_large = item.get("IMAGES_LARGE", {}).get("IMAGE_URL_LARGE", [])
        
        if isinstance(images, str):
            images = [images]
        if isinstance(images_large, str):
            images_large = [images_large]
        
        max_len = max(len(images), len(images_large))
        images += [None] * (max_len - len(images))
        images_large += [None] * (max_len - len(images_large))
        
        # Verificar se o item já existe no banco
        existing_item = supabase.table("product").select("*").eq("external_id", external_id).execute()
        existing_data = existing_item.data[0] if existing_item.data else None
        
        product_data = {
            "title": title,
            "category": category,
            "description": description,
            "make": make,
            "model": model,
            "base_model": base_model,
            "year": year,
            "manufacture_year": manufacture_year,
            "condition": condition,
            "mileage": mileage,
            "fuel_type": fuel_type,
            "transmission": transmission,
            "engine": engine,
            "plate": plate,
            "chassis": chassis,
            "doors": doors,
            "color": color,
            "price": price,
            "promotion_price": promotion_price,
            "horsepower": horsepower,
            "body_type": body_type,
            "fipe_code": fipe_code,
            "fipe_value": fipe_value
        }
        
        if not existing_data:
            # Inserir novo item na tabela 'product'
            product_data.update({
                "vehicle_id": vehicle_id,
                "external_id": external_id,
                "date_added": datetime.now().strftime('%Y-%m-%d')
            })
            try:
                supabase.table("product").insert(product_data).execute()
                logging.info(f"Novo produto inserido: {title}")
            except Exception as e:
                logging.error(f"Erro ao inserir o produto '{title}': {e}")
                continue

            # Inserir acessórios na tabela 'product_accessories'
            accessories = item.get("ACCESSORIES", "") or ""
            for accessory in accessories.split(","):
                accessory = accessory.strip()
                if accessory:
                    accessory_data = {
                        "vehicle_id": vehicle_id,
                        "accessory": accessory
                    }
                    try:
                        supabase.table("product_accessories").insert(accessory_data).execute()
                    except Exception as e:
                        logging.error(f"Erro ao inserir acessório '{accessory}' para o produto '{title}': {e}")

            # Inserir imagens na tabela 'product_images'
            for img_url, img_large_url in zip(images, images_large):
                if img_url:
                    image_data = {
                        "vehicle_id": vehicle_id,
                        "image_url": img_url,
                        "image_url_large": img_large_url if img_large_url else None
                    }
                    try:
                        supabase.table("product_images").insert(image_data).execute()
                    except Exception as e:
                        logging.error(f"Erro ao inserir imagem para o produto '{title}': {e}")

        else:
            vehicle_id = existing_data.get("vehicle_id")
            # Comparar os dados do produto
            update_required = False
            for key, value in product_data.items():
                existing_value = existing_data.get(key)
                if existing_value != value:
                    update_required = True
                    break

            if update_required:
                # Atualizar o produto
                try:
                    supabase.table("product").update(product_data).eq("external_id", external_id).execute()
                    logging.info(f"Produto atualizado: {title}")
                except Exception as e:
                    logging.error(f"Erro ao atualizar o produto '{title}': {e}")

                # Redefinir o campo 'download' para None para recriar o ZIP
                supabase.table("product").update({"download": None}).eq("vehicle_id", vehicle_id).execute()
            else:
                logging.info(f"Produto '{title}' já está atualizado. Nenhuma ação necessária.")

            # Verificar e atualizar acessórios
            accessories = item.get("ACCESSORIES", "") or ""
            accessories_list = [a.strip() for a in accessories.split(",") if a.strip()]
            existing_accessories_response = supabase.table("product_accessories").select("accessory").eq("vehicle_id", vehicle_id).execute()
            existing_accessories = [a["accessory"] for a in existing_accessories_response.data]

            if set(accessories_list) != set(existing_accessories):
                try:
                    supabase.table("product_accessories").delete().eq("vehicle_id", vehicle_id).execute()
                    for accessory in accessories_list:
                        accessory_data = {
                            "vehicle_id": vehicle_id,
                            "accessory": accessory
                        }
                        supabase.table("product_accessories").insert(accessory_data).execute()
                    logging.info(f"Acessórios atualizados para o produto: {title}")
                    supabase.table("product").update({"download": None}).eq("vehicle_id", vehicle_id).execute()
                except Exception as e:
                    logging.error(f"Erro ao atualizar acessórios para o produto '{title}': {e}")
            else:
                logging.info(f"Acessórios do produto '{title}' já estão atualizados.")

            # Verificar e atualizar imagens
            existing_images_response = supabase.table("product_images").select("image_url, image_url_large").eq("vehicle_id", vehicle_id).execute()
            existing_images = [(img["image_url"], img.get("image_url_large")) for img in existing_images_response.data]
            new_images = list(zip(images, images_large))

            if existing_images != new_images:
                try:
                    supabase.table("product_images").delete().eq("vehicle_id", vehicle_id).execute()
                    for img_url, img_large_url in new_images:
                        if img_url:
                            image_data = {
                                "vehicle_id": vehicle_id,
                                "image_url": img_url,
                                "image_url_large": img_large_url if img_large_url else None
                            }
                            supabase.table("product_images").insert(image_data).execute()
                    logging.info(f"Imagens atualizadas para o produto: {title}")
                    supabase.table("product").update({"download": None}).eq("vehicle_id", vehicle_id).execute()
                except Exception as e:
                    logging.error(f"Erro ao atualizar imagens para o produto '{title}': {e}")
            else:
                logging.info(f"Imagens do produto '{title}' já estão atualizadas.")

    # Remover produtos que não estão no XML
    supabase_products_response = supabase.table("product").select("external_id, vehicle_id, title").execute()
    supabase_products = supabase_products_response.data
    supabase_external_ids = {item["external_id"] for item in supabase_products}

    external_ids_to_remove = supabase_external_ids - xml_external_ids

    for external_id in external_ids_to_remove:
        product_to_remove = next((item for item in supabase_products if item["external_id"] == external_id), None)
        if product_to_remove:
            vehicle_id = product_to_remove["vehicle_id"]
            vehicle_title = product_to_remove["title"]

            # Remover acessórios, imagens e produto
            supabase.table("product_accessories").delete().eq("vehicle_id", vehicle_id).execute()
            supabase.table("product_images").delete().eq("vehicle_id", vehicle_id).execute()
            supabase.table("product").delete().eq("vehicle_id", vehicle_id).execute()
            logging.info(f"Produto removido: {vehicle_title}")

            # Remover arquivo ZIP do bucket
            zip_filename = f"{vehicle_title.replace(' ', '_')}_images.zip"
            try:
                supabase.storage.from_(BUCKET_NAME).remove([zip_filename])
                logging.info(f"Arquivo ZIP '{zip_filename}' removido do bucket.")
            except Exception as e:
                logging.error(f"Erro ao remover o arquivo ZIP '{zip_filename}' do bucket: {e}")

# Função para criar um ZIP com as fotos de cada veículo e atualizar o link de download no Supabase
def create_vehicle_image_zip():
    # Cria o diretório de download, se ele não existir
    os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

    # Obter a lista de veículos e suas imagens do Supabase
    vehicles = supabase.table("product").select("vehicle_id, title, download").execute()
    
    for vehicle in vehicles.data:
        vehicle_id = vehicle["vehicle_id"]
        vehicle_title = vehicle["title"]
        download_link = vehicle.get("download")

        # Definir o nome do arquivo ZIP esperado
        zip_filename = f"{vehicle_title.replace(' ', '_')}_images.zip"
        expected_download_url = supabase.storage.from_(BUCKET_NAME).get_public_url(zip_filename)

        # Verificar se o arquivo ZIP já existe no bucket
        zip_exists = zip_exists_in_bucket(zip_filename)

        # Verificar se o link de download no banco está correto
        download_link_correct = download_link == expected_download_url

        if zip_exists and not download_link_correct:
            # O ZIP existe, mas o link está faltando ou incorreto; atualizar o link no banco
            try:
                supabase.table("product").update({"download": expected_download_url}).eq("vehicle_id", vehicle_id).execute()
                logging.info(f"Link de download atualizado para o veículo '{vehicle_title}': {expected_download_url}")
            except Exception as e:
                logging.error(f"Erro ao atualizar o link de download para o veículo '{vehicle_title}': {e}")
            continue

        elif zip_exists and download_link_correct:
            # O ZIP existe e o link está correto; não precisa fazer nada
            logging.info(f"Veículo '{vehicle_title}' já possui o ZIP e o link de download correto. Pulando processamento.")
            continue

        else:
            # O ZIP não existe; precisa ser criado e o link atualizado
            images_response = supabase.table("product_images").select("image_url").eq("vehicle_id", vehicle_id).execute()
            image_urls = [img["image_url"] for img in images_response.data]

            if not image_urls:
                logging.warning(f"Nenhuma imagem encontrada para o veículo '{vehicle_title}'. Pulando.")
                continue

            # Pasta temporária para imagens do veículo
            vehicle_folder = os.path.join(DOWNLOAD_FOLDER, vehicle_title.replace(" ", "_"))
            os.makedirs(vehicle_folder, exist_ok=True)

            # Baixar cada imagem
            for idx, url in enumerate(image_urls, start=1):
                image_path = os.path.join(vehicle_folder, f"{vehicle_title.replace(' ', '_')}_image_{idx}.jpg")
                download_image(url, image_path)

            # Compactar todas as imagens em um único arquivo ZIP
            zip_path = os.path.join(DOWNLOAD_FOLDER, zip_filename)
            try:
                with ZipFile(zip_path, "w") as zipf:
                    for root, _, files in os.walk(vehicle_folder):
                        for file in files:
                            file_path = os.path.join(root, file)
                            zipf.write(file_path, os.path.relpath(file_path, DOWNLOAD_FOLDER))
                logging.info(f"Arquivo ZIP criado para o veículo '{vehicle_title}': {zip_filename}")
            except Exception as e:
                logging.error(f"Erro ao criar o ZIP para o veículo '{vehicle_title}': {e}")
                continue

            # Fazer upload do arquivo ZIP para o bucket no Supabase
            download_url = upload_zip_to_bucket(zip_path, zip_filename)

            if not download_url:
                logging.error(f"Falha ao obter o link de download para o veículo '{vehicle_title}'. Pulando atualização.")
                continue

            # Atualizar o link de download no registro do veículo na tabela `product`
            try:
                supabase.table("product").update({"download": download_url}).eq("vehicle_id", vehicle_id).execute()
                logging.info(f"Link de download adicionado para o veículo '{vehicle_title}': {download_url}")
            except Exception as e:
                logging.error(f"Erro ao atualizar o link de download para o veículo '{vehicle_title}': {e}")

            # Limpar a pasta temporária de imagens após compactação e upload
            try:
                for root, _, files in os.walk(vehicle_folder):
                    for file in files:
                        os.remove(os.path.join(root, file))
                os.rmdir(vehicle_folder)
                os.remove(zip_path)
                logging.info(f"Arquivos temporários removidos para o veículo '{vehicle_title}'.")
            except Exception as e:
                logging.error(f"Erro ao limpar arquivos temporários para o veículo '{vehicle_title}': {e}")

    logging.info("Processo concluído. Arquivos ZIP gerados e links de download atualizados para cada veículo.")

def main():
    logging.info("Buscando dados do XML...")
    data = fetch_xml_data(XML_URL)
    
    if data:
        logging.info("Atualizando dados no Supabase a partir do XML...")
        update_supabase_from_xml(data)
    else:
        logging.error("Falha ao obter dados do XML. Encerrando o processo.")
        return

    logging.info("Criando e atualizando arquivos ZIP com as imagens dos veículos...")
    create_vehicle_image_zip()

if __name__ == "__main__":
    main()
