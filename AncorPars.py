import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz
from google.oauth2 import service_account
from googleapiclient.discovery import build
import re
import gspread
from urllib.parse import urljoin, urlparse, urlparse

# Налаштування сторінки
st.set_page_config(
    page_title="Перевірка розміщення текстів",
    page_icon="📄",
    layout="wide"
)

# Ініціалізація session state
if 'data' not in st.session_state:
    st.session_state.data = None
if 'selected_row' not in st.session_state:
    st.session_state.selected_row = None

# Функція для отримання Google Docs API клієнта
@st.cache_resource
def get_docs_service():
    try:
        credentials = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/documents.readonly"]
        )
        service = build('docs', 'v1', credentials=credentials)
        return service
    except Exception as e:
        st.error(f"Помилка підключення до Google Docs API: {e}")
        return None

# Функція для завантаження даних з Google Sheets
@st.cache_data(ttl=600)
def load_data_from_sheets():
    try:
        # Используем gspread + service account из st.secrets
        creds = service_account.Credentials.from_service_account_info(
            st.secrets["gcp_service_account"],
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )

        client = gspread.Client(auth=creds)

        spreadsheet = st.secrets["connections"]["gsheets"]["spreadsheet"]
        # Поддержка URL или ID
        if isinstance(spreadsheet, str) and spreadsheet.startswith("http"):
            sh = client.open_by_url(spreadsheet)
        else:
            sh = client.open_by_key(spreadsheet)

        worksheet = sh.get_worksheet(0)
        
        # Читаємо всі дані як список списків, щоб уникнути помилки з дублікатами в заголовках
        all_values = worksheet.get_all_values()
        
        if not all_values:
            return pd.DataFrame()

        # Створюємо DataFrame з усіма даними
        df = pd.DataFrame(all_values)
        
        # Отримуємо перший рядок (заголовки) і перетворюємо його на список
        header = df.iloc[0].tolist()
        
        # Створюємо унікальні імена для колонок
        new_columns = []
        col_counts = {}
        for i, col in enumerate(header):
            # Якщо заголовок порожній, даємо йому тимчасове ім'я
            if not col:
                col = f'Unnamed.{i}'
            # Якщо такий заголовок вже є, додаємо суфікс
            if col in col_counts:
                col_counts[col] += 1
                new_columns.append(f"{col}_{col_counts[col]}")
            else:
                col_counts[col] = 0
                new_columns.append(col)
        
        # Призначаємо нові, унікальні заголовки
        df.columns = new_columns
        
        # Видаляємо перший рядок, який був заголовком
        df = df.iloc[1:].reset_index(drop=True)

        # Ограничение колонок аналогично usecols=list(range(10))
        if df.shape[1] > 10:
            df = df.iloc[:, :10]

        # Очистка данных
        df = df.dropna(subset=['Посилання на відповідь анотацію, анкор', 'Посилання на сайті'])
        df = df.reset_index(drop=True)

        return df
    except Exception as e:
        st.error(f"Помилка завантаження даних: {e}")
        return None

# Функція для витягування document ID з URL
def extract_doc_id(url):
    if not url or not isinstance(url, str):
        return None
    # Поддерживаем разные форматы ссылки: /d/<id>/, ?id=<id>, и т.п.
    match = re.search(r'(?:/d/|/document/d/|[?&]id=)([-\w]+)', url)
    return match.group(1) if match else None

# Функція для отримання тексту з Google Docs
@st.cache_data(ttl=3600)
def get_doc_text(doc_url):
    if not doc_url:
        return ""
    
    doc_id = extract_doc_id(doc_url)
    if not doc_id:
        return ""
    
    try:
        service = get_docs_service()
        if not service:
            return ""
        
        document = service.documents().get(documentId=doc_id).execute()
        
        # Витягування тексту
        content = document.get('body', {}).get('content', [])
        text_parts = []
        
        for element in content:
            if 'paragraph' in element:
                paragraph = element['paragraph']
                for text_run in paragraph.get('elements', []):
                    if 'textRun' in text_run:
                        text_parts.append(text_run['textRun']['content'])
            elif 'table' in element:
                table = element['table']
                for row in table.get('tableRows', []):
                    for cell in row.get('tableCells', []):
                        for cell_content in cell.get('content', []):
                            if 'paragraph' in cell_content:
                                for text_run in cell_content['paragraph'].get('elements', []):
                                    if 'textRun' in text_run:
                                        text_parts.append(text_run['textRun']['content'])
        
        text = ''.join(text_parts)
        # Очищення тексту
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    except Exception as e:
        st.warning(f"Помилка читання документа {doc_id}: {e}")
        return ""

# Функція для перевірки наявності редакторів на сторінці
def check_editors_on_page(page_text):
    """
    Перевіряє наявність хоча б одного редактора на сторінці
    Редактори: "Щербаченко Юлія" або "Севрюков Олександр Вікторович"
    Повертає список всіх знайдених редакторів
    """
    if not page_text:
        return False, []
    
    editors = [
        "Щербаченко Юлія",
        "Севрюков Олександр Вікторович"
    ]
    
    # Нормалізуємо текст для пошуку (замінюємо неразрывные пробіли)
    normalized_text = page_text.replace('\xa0', ' ')
    
    found_editors = []
    for editor in editors:
        # Шукаємо редактора в тексті (ігноруючи регістр)
        if re.search(re.escape(editor), normalized_text, re.IGNORECASE):
            found_editors.append(editor)
    
    return len(found_editors) > 0, found_editors

# Функція для витягування блоку "Список использованной литературы" та посилань зі сторінки сайту
@st.cache_data(ttl=3600)
def extract_references_section(page_url):
    """
    Витягує блок 'Список использованной литературы' зі сторінки сайту та повертає список посилань
    Обмежує пошук тільки блоком після заголовка до наступного великого розділу
    """
    if not page_url:
        return []
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(page_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Отримуємо домен поточної сторінки для фільтрації внутрішніх посилань
        page_domain = None
        try:
            parsed = urlparse(page_url)
            page_domain = parsed.netloc.lower()
        except:
            pass
        
        # Варіанти назв розділу
        section_patterns = [
            r'Список использованной литературы',
            r'Список використаної літератури',
            r'Список литературы',
            r'Список літератури',
            r'Литература',
            r'Література',
            r'References',
            r'Bibliography'
        ]
        
        # Знаходимо заголовок розділу
        section_header = None
        
        # Спочатку шукаємо в заголовках (h1-h6)
        for tag in soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = tag.get_text(strip=True)
            if not text:
                continue
                
            text_lower = text.lower()
            for pattern in section_patterns:
                if re.search(pattern, text_lower, re.IGNORECASE):
                    section_header = tag
                    break
            
            if section_header:
                break
        
        # Якщо не знайдено в заголовках, шукаємо в параграфах та div (короткі тексти)
        if not section_header:
            for tag in soup.find_all(['p', 'div', 'section', 'span']):
                text = tag.get_text(strip=True)
                if not text or len(text) > 100:  # Заголовок зазвичай короткий
                    continue
                    
                text_lower = text.lower()
                for pattern in section_patterns:
                    if re.search(pattern, text_lower, re.IGNORECASE):
                        section_header = tag
                        break
                
                if section_header:
                    break
        
        references_links = []
        
        if section_header:
            # Знаходимо наступні sibling елементи після заголовка
            # Обмежуємо пошук максимум 20 елементами після заголовка
            section_content = []
            count = 0
            max_elements = 20
            
            # Шукаємо наступні елементи після заголовка
            current = section_header.next_sibling
            while current and count < max_elements:
                # Перевіряємо, чи це тег (не просто текстовий вузол)
                if hasattr(current, 'name') and current.name:
                    # Зупиняємося на наступному великому заголовку
                    if current.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                        break
                    # Додаємо тільки елементи, що можуть містити посилання
                    if current.name in ['p', 'div', 'li', 'ul', 'ol', 'span', 'a', 'section']:
                        section_content.append(current)
                        count += 1
                current = current.next_sibling
            
            # Якщо не знайдено достатньо елементів через siblings, шукаємо в батьківському контейнері
            if len(section_content) < 3:
                parent_container = section_header.find_parent(['div', 'section', 'article', 'main'])
                if parent_container:
                    # Знаходимо всі елементи в контейнері після заголовка
                    found_header = False
                    count = 0
                    
                    for elem in parent_container.find_all(['p', 'div', 'li', 'span', 'a', 'ul', 'ol']):
                        if elem == section_header:
                            found_header = True
                            continue
                        
                        if found_header and count < max_elements:
                            # Зупиняємося на наступному великому заголовку
                            if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                                break
                            if elem not in section_content:
                                section_content.append(elem)
                                count += 1
            
            # Витягуємо посилання тільки зі знайденого блоку
            for elem in section_content:
                # Шукаємо посилання в тегах <a>
                for link in elem.find_all('a', href=True):
                    href = link.get('href', '')
                    if href and not href.startswith('#') and not href.startswith('javascript:'):
                        if href.startswith('http://') or href.startswith('https://'):
                            references_links.append(href)
                        elif href.startswith('/'):
                            full_url = urljoin(page_url, href)
                            references_links.append(full_url)
                        else:
                            full_url = urljoin(page_url, href)
                            references_links.append(full_url)
                
                # Шукаємо URL в тексті (формат "текст / domain.com" або повний URL)
                elem_text = elem.get_text(separator='\n')
                
                # Розбиваємо текст на рядки для обробки формату "текст / domain.com"
                lines = elem_text.split('\n')
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue
                    
                    # Патерн для повних URL
                    full_url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s<>"{}|\\^`\[\].,;:!?]'
                    found_urls = re.findall(full_url_pattern, line)
                    references_links.extend(found_urls)
                    
                    # Патерн для доменів у форматі "текст / domain.com" або просто "domain.com"
                    # Шукаємо патерн типу "текст / domain.com" або просто "domain.com"
                    # Але тільки якщо це не внутрішній домен
                    # Патерн для формату "текст / domain.com" або "domain.com;"
                    domain_pattern = r'([a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9]*\.[a-zA-Z]{2,}(?:/[^\s<>"{}|\\^`\[\]]*)?)'
                    found_domains = re.findall(domain_pattern, line)
                    for domain in found_domains:
                        domain = domain.strip(' /;.,')
                        # Пропускаємо, якщо це вже повний URL
                        if not domain.startswith('http'):
                            # Перевіряємо, чи це не внутрішній домен
                            try:
                                domain_netloc = urlparse('https://' + domain).netloc.lower()
                                if page_domain and (domain_netloc == page_domain or domain_netloc.endswith('.' + page_domain)):
                                    continue
                            except:
                                pass
                            # Додаємо протокол
                            full_url = 'https://' + domain
                            references_links.append(full_url)
        
        # Фільтруємо посилання - видаляємо внутрішні посилання сайту та нормалізуємо до доменів
        filtered_domains = set()  # Використовуємо set для автоматичного видалення дублікатів
        
        for link in references_links:
            try:
                # Парсимо посилання
                link_parsed = urlparse(link)
                link_domain = link_parsed.netloc.lower()
                
                # Пропускаємо порожні або невалідні посилання
                if not link_domain:
                    continue
                
                # Пропускаємо внутрішні посилання
                if page_domain:
                    if link_domain == page_domain or link_domain.endswith('.' + page_domain):
                        continue
                
                # Видаляємо www. з домену
                if link_domain.startswith('www.'):
                    link_domain = link_domain[4:]
                
                # Створюємо нормалізоване посилання тільки з доменом
                normalized_link = 'https://' + link_domain
                filtered_domains.add(normalized_link)
                
            except Exception:
                continue
        
        # Конвертуємо set в список та сортуємо
        unique_links = sorted(list(filtered_domains))
        
        return unique_links
    
    except Exception as e:
        st.warning(f"Помилка витягування списку літератури зі сторінки {page_url}: {e}")
        return []

# Функція для отримання тексту зі сторінки
@st.cache_data(ttl=3600)
def get_page_text(page_url):
    if not page_url:
        return ""
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        response = requests.get(page_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Видалення скриптів та стилів
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()
        
        text = soup.get_text(separator='\n')
        # Очищення тексту
        text = text.lower()
        text = text.replace('\xa0', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text
    except Exception as e:
        st.warning(f"Помилка завантаження сторінки {page_url}: {e}")
        return ""





# Витягнути фрагмент між маркерами 'Аннотация' і 'Анкор'
def extract_annotation_fragment(text: str) -> str:
    """
    Витягує фрагмент "Аннотация" з документа для перевірки російської локалі
    """
    if not text or not isinstance(text, str):
        return ""

    # Нормалізуємо пробіли та прибираємо маркери форматування для пошуку (як у українській версії)
    t = text.replace('\xa0', ' ')
    t = re.sub(r'\s+', ' ', t)

    # Варіанти заголовків (кирилиця): Аннотация / Аннотація
    # Важливо: шукаємо тільки "Аннотация" БЕЗ "Перевод" перед ним
    start_patterns = [r'Аннотация', r'Аннотація']
    # Кінцевий маркер: спочатку шукаємо "Анкор", потім "Перевод аннотации" як резерв
    end_patterns_primary = [r'Анкор', r'Anchor', r'Анкoр']
    end_patterns_secondary = [r'Перевод аннотации', r'Переклад аннотації', r'Перевод аннотації', r'Переклад аннотации']

    # Побудуємо regex для пошуку позицій (ігноруючи регістр і можливі двокрапки/зірочки)
    # Шукаємо всі входження "Аннотация", а потім перевіряємо, чи перед ним немає "Перевод"
    start_re = re.compile(r'(' + '|'.join(start_patterns) + r')\s*[:\-–—]*', flags=re.IGNORECASE)
    
    # Знаходимо всі можливі входження
    all_matches = list(start_re.finditer(t))
    
    # Шукаємо перше входження, перед яким НЕ стоїть "Перевод" або "Переклад"
    start_match = None
    for match in all_matches:
        start_idx = match.start()
        # Перевіряємо, чи перед "Аннотация" немає "Перевод" або "Переклад"
        before_text = t[max(0, start_idx-20):start_idx].lower()
        if 'перевод' not in before_text and 'переклад' not in before_text:
            start_match = match
            break
    
    if not start_match:
        # Якщо маркер не знайдено, повертаємо весь текст (як було раніше)
        return t.strip()

    # Шукаємо перший матч end після start
    start_pos = start_match.end()
    
    # Спочатку шукаємо основний кінцевий маркер "Анкор"
    end_re_primary = re.compile(r'(' + '|'.join(end_patterns_primary) + r')\s*[:\-–—]*', flags=re.IGNORECASE)
    end_match = end_re_primary.search(t, pos=start_pos)
    
    # Якщо не знайдено "Анкор", шукаємо "Перевод аннотации" як резерв
    if not end_match:
        end_re_secondary = re.compile(r'(' + '|'.join(end_patterns_secondary) + r')\s*[:\-–—]*', flags=re.IGNORECASE)
        end_match = end_re_secondary.search(t, pos=start_pos)

    if end_match:
        fragment = t[start_pos:end_match.start()]
    else:
        # Якщо енд маркер не знайдено — беремо решту тексту після Аннотации
        fragment = t[start_pos:]

    # Повертаємо "сирий" фрагмент, очищення буде в compare_texts
    return fragment.strip()

# Витягнути фрагмент "Перевод аннотации" для української локалі
def extract_ukrainian_annotation_fragment(text: str) -> str:
    """
    Витягує фрагмент "Перевод аннотации" з документа для перевірки української локалі
    """
    if not text or not isinstance(text, str):
        return ""

    # Нормалізуємо пробіли та прибираємо маркери форматування для пошуку
    t = text.replace('\xa0', ' ')
    t = re.sub(r'\s+', ' ', t)

    # Варіанти заголовків для української версії
    start_patterns = [r'Перевод аннотации', r'Переклад аннотації', r'Перевод аннотації', r'Переклад аннотации']
    # Кінцевий маркер може бути "Анкор" або кінець документа
    end_patterns = [r'Анкор', r'Anchor', r'Анкoр']

    # Побудуємо regex для пошуку позицій (ігноруючи регістр і можливі двокрапки/зірочки)
    start_re = re.compile(r'(' + '|'.join(start_patterns) + r')\s*[:\-–—]*', flags=re.IGNORECASE)
    end_re = re.compile(r'(' + '|'.join(end_patterns) + r')\s*[:\-–—]*', flags=re.IGNORECASE)

    start_match = start_re.search(t)
    if not start_match:
        return ""

    # Шукаємо перший матч end після start
    start_pos = start_match.end()
    end_match = end_re.search(t, pos=start_pos)

    if end_match:
        fragment = t[start_pos:end_match.start()]
    else:
        # Якщо енд маркер не знайдено — беремо решту тексту після "Перевод аннотации"
        fragment = t[start_pos:]

    # Очистимо фрагмент від зайвих зірочок/меток і обріжемо
    fragment = re.sub(r'[\*_#]{1,}', ' ', fragment)
    fragment = re.sub(r'\s+', ' ', fragment).strip()

    return fragment

# Функція для генерації української URL з російської
def generate_ukrainian_url(ru_url: str) -> str:
    """
    Генерує українську URL з російської, додаючи /ua/ перед шляхом
    Приклад: https://apteka911.ua/shop/... -> https://apteka911.ua/ua/shop/...
    """
    if not ru_url or not isinstance(ru_url, str):
        return ""
    
    try:
        parsed = urlparse(ru_url)
        path = parsed.path
        
        # Якщо URL вже містить /ua/, повертаємо як є
        if '/ua/' in path:
            return ru_url
        
        # Додаємо /ua/ перед шляхом
        if path.startswith('/'):
            new_path = '/ua' + path
        else:
            new_path = '/ua/' + path
        
        # Формуємо нову URL
        new_url = f"{parsed.scheme}://{parsed.netloc}{new_path}"
        if parsed.query:
            new_url += f"?{parsed.query}"
        if parsed.fragment:
            new_url += f"#{parsed.fragment}"
        
        return new_url
    except Exception as e:
        return ""

# Функція для генерації російської URL з української
def generate_russian_url(url: str) -> str:
    """
    Генерує російську URL з української, видаляючи /ua/ зі шляху.
    Приклад: https://apteka911.ua/ua/shop/... -> https://apteka911.ua/shop/...
    """
    if not url or not isinstance(url, str):
        return ""
    
    try:
        # Використовуємо urlparse для надійної маніпуляції шляхом
        parsed = urlparse(url)
        path = parsed.path
        
        new_path = path
        # Перевіряємо, чи шлях починається з /ua/ або дорівнює /ua
        if path.startswith('/ua/'):
            new_path = path[3:]  # Видаляємо /ua, залишаючи / на початку
        elif path == '/ua':
            new_path = '/' # Якщо шлях був просто /ua, робимо його кореневим
        
        # Збираємо URL назад
        from urllib.parse import urlunparse
        new_url_parts = list(parsed)
        new_url_parts[2] = new_path
        return urlunparse(new_url_parts)
        
    except Exception:
        return ""

# Спрощене очищення тексту для порівняння (без видалення важливих частин)
def clean_text_for_comparison(text: str) -> str:
    """
    Спрощене очищення тексту для порівняння, без видалення важливих частин
    """
    if not text or not isinstance(text, str):
        return ""
    
    # Replace non-breaking spaces and normalize newlines/spaces
    t = text.replace('\xa0', ' ')
    t = t.replace('\r\n', '\n').replace('\r', '\n')
    
    # Remove HTML tags
    t = re.sub(r'<[^>]+>', ' ', t)
    
    # Remove bold/italic markers like **bold**, __italic__, *italic*
    t = re.sub(r'(\*\*|__)(.*?)\1', r'\2', t)
    t = re.sub(r'(?<!\*)\*(?!\*)', ' ', t)
    t = re.sub(r'_(.*?)_', r'\1', t)
    
    # Remove markdown headings (# Heading)
    t = re.sub(r'(?m)^\s{0,3}#+\s*', ' ', t)
    
    # Remove footnote markers like [1], [^1]
    t = re.sub(r'\[\^?.*?\]', ' ', t)
    
    # Remove multiple punctuation sequences
    t = re.sub(r'[\-\u2014]{2,}', ' ', t)
    t = re.sub(r'\.\.{2,}', ' ', t)
    
    # Remove leftover asterisks and underscores
    t = t.replace('*', ' ').replace('_', ' ')
    
    # Collapse whitespace and trim
    t = re.sub(r'\s+', ' ', t).strip()
    
    return t

# Функція для порівняння текстів
def compare_texts(doc_fragment, page_text, threshold=75, chunk_size=300):
    """
    Порівнює фрагмент документа з текстом сторінки
    doc_fragment - вже витягнутий фрагмент (Аннотация або Перевод аннотации)
    """
    if not doc_fragment or not page_text:
        return 0.0, 0.0, []

    # Очищаємо витягнутий фрагмент і текст сторінки перед порівнянням
    # Використовуємо спрощене очищення, щоб не видаляти важливі частини
    doc_text_clean = clean_text_for_comparison(doc_fragment)
    page_text_clean = clean_text_for_comparison(page_text)

    doc_text_lower = doc_text_clean.lower()
    page_text_lower = page_text_clean.lower()

    # Розбиття на частини (по словах) з невеликим перекриттям
    words = doc_text_lower.split()
    if not words:
        return 0.0, 0.0, []

    chunks = []
    # Делают шаг равный 80% размера фрагмента -> 20% overlap
    step = max(1, int(chunk_size * 0.8))

    # Використовуємо очищений текст для обох списків, щоб індекси співпадали
    orig_words_clean = doc_text_clean.split()
    min_words_threshold = 10

    for i in range(0, len(words), step):
        chunk_words = words[i:i + chunk_size]
        if len(chunk_words) >= min_words_threshold:
            # Беремо оригінальні слова з очищеного фрагмента (для відображення)
            orig_chunk_words = orig_words_clean[i:i + chunk_size] if i < len(orig_words_clean) else []
            original = ' '.join(orig_chunk_words) if orig_chunk_words else ' '.join(chunk_words)
            normalized = ' '.join(chunk_words)
            chunks.append({
                'original': original,
                'normalized': normalized
            })

    if not chunks:
        return 0.0, 0.0, []

    # Обчислення схожості для кожного фрагмента
    scores = []
    missing_fragments = []

    for chunk in chunks:
        # Використовуємо token_set_ratio для кращого порівняння
        score = fuzz.token_set_ratio(chunk['normalized'], page_text_lower)
        scores.append(score)

        if score < threshold:
            missing_fragments.append({
                'text': chunk['original'][:200] + '...' if len(chunk['original']) > 200 else chunk['original'],
                'score': score
            })

    min_score = min(scores) if scores else 0.0
    avg_score = sum(scores) / len(scores) if scores else 0.0

    return min_score, avg_score, missing_fragments

# Заголовок
st.title("📄 Перевірка розміщення текстів з Google Docs")
st.markdown("---")

# Бічна панель з налаштуваннями
with st.sidebar:
    st.header("⚙️ Налаштування")
    
    threshold = st.slider(
        "Поріг подібності (%)",
        min_value=0,
        max_value=100,
        value=75,
        help="Мінімальний відсоток схожості для вважання тексту розміщеним"
    )
    
    chunk_size = st.slider(
        "Розмір фрагмента (слова)",
        min_value=100,
        max_value=500,
        value=300,
        step=50,
        help="Кількість слів у кожному фрагменті для перевірки"
    )
    
    show_problems_only = st.checkbox(
        "Показати тільки проблемні записи",
        value=False
    )
    
    st.markdown("---")
    
    # Кнопка оновлення
    if st.button("🔄 Оновити дані", use_container_width=True):
        st.cache_data.clear()
        st.session_state.data = None
        st.rerun()
    
    st.markdown("---")
    st.markdown("### 📊 Легенда")
    st.markdown(f"""
    - 🟢 **Добре**: ≥ {threshold}%
    - 🟡 **Увага**: {threshold-15}–{threshold-1}%
    - 🔴 **Проблема**: < {threshold-15}%
    """)

# Завантаження даних
if st.session_state.data is None:
    with st.spinner("Завантаження даних з Google Sheets..."):
        st.session_state.data = load_data_from_sheets()

df = st.session_state.data

if df is not None and not df.empty:
    st.success(f"✅ Завантажено {len(df)} записів")
    
    # Кнопка для запуску перевірки
    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        if st.button("🚀 Запустити перевірку всіх записів", type="primary", use_container_width=True):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            results = []
            for idx, row in df.iterrows():
                status_text.text(f"Перевірка {idx + 1} з {len(df)}: {row.get('Питання UKR', 'N/A')}")

                doc_url = row.get('Посилання на відповідь анотацію, анкор', '')
                base_page_url = row.get('Посилання на сайті', '') or row.get('Посилання', '') or row.get('Посилання на сторінці', '') or row.get('URL', '') or ''

                # Перевірка валідності URL
                if not base_page_url or not (base_page_url.startswith('http://') or base_page_url.startswith('https://')):
                    st.warning(f"Пропущено запис {idx + 1} через невалідний URL: {base_page_url}")
                    results.append({
                        'Питання UKR': row.get('Питання UKR', 'N/A'),
                        'Статус': 'Помилка',
                        'Деталі': f"Невалідний або відсутній URL: {base_page_url}",
                        'page_url_ru': base_page_url,
                        'page_url_ua': base_page_url,
                        'doc_url': doc_url,
                        'score_ru': 0,
                        'score_ua': 0,
                        'has_editor_ru': False,
                        'has_editor_ua': False,
                        'found_editors_ru': [],
                        'found_editors_ua': [],
                        'references_links_ru': [],
                        'references_links_ua': []
                    })
                    continue

                # Генерація URL для кожної локалі
                page_url_ru = generate_russian_url(base_page_url)
                page_url_ua = generate_ukrainian_url(base_page_url)

                # Отримання текстів для російської локалі
                doc_text = get_doc_text(doc_url)
                page_text_ru = get_page_text(page_url_ru)
                
                # Витягування фрагмента "Аннотация" для російської локалі
                doc_fragment_ru = extract_annotation_fragment(doc_text)
                
                # Перевірка: якщо фрагмент занадто довгий (більше 80% від всього тексту),
                # можливо маркер не знайдено і повернуто весь текст
                if doc_fragment_ru and len(doc_fragment_ru) > len(doc_text) * 0.8:
                    # Спробуємо знайти "Аннотация" вручну
                    annotation_match = re.search(r'Аннотация\s*:\s*(.*?)(?=\s*(?:Анкор|Перевод|$))', doc_text, re.IGNORECASE | re.DOTALL)
                    if annotation_match:
                        doc_fragment_ru = annotation_match.group(1).strip()
                
                # Витягування посилань з блоку "Список использованной литературы" зі сторінки сайту (російська)
                references_links_ru = extract_references_section(page_url_ru)
                
                # Перевірка наявності редакторів на сторінці (російська)
                has_editor_ru, found_editors_ru = check_editors_on_page(page_text_ru)

                # Порівняння для російської локалі
                min_sim_ru, avg_sim_ru, missing_ru = compare_texts(
                    doc_fragment_ru, page_text_ru, threshold, chunk_size
                )

                # Перевірка української локалі
                page_text_ua = get_page_text(page_url_ua) if page_url_ua else ""
                
                # Витягування фрагмента "Перевод аннотации" для української локалі
                doc_fragment_ua = extract_ukrainian_annotation_fragment(doc_text)
                
                # Витягування посилань з блоку "Список использованной литературы" зі сторінки сайту (українська)
                references_links_ua = extract_references_section(page_url_ua) if page_url_ua else []
                
                # Перевірка наявності редакторів на сторінці (українська)
                has_editor_ua, found_editors_ua = check_editors_on_page(page_text_ua) if page_text_ua else (False, [])

                # Порівняння для української локалі
                min_sim_ua, avg_sim_ua, missing_ua = compare_texts(
                    doc_fragment_ua, page_text_ua, threshold, chunk_size
                ) if doc_fragment_ua and page_text_ua else (0.0, 0.0, [])

                # Сохраняем только превью текстов и ссылки — полные тексты будем подгружать при раскрытии
                results.append({
                    'index': idx,
                    # Російська локаль
                    'min_similarity_ru': round(min_sim_ru, 1),
                    'avg_similarity_ru': round(avg_sim_ru, 1),
                    'missing_count_ru': len(missing_ru),
                    'missing_fragments_ru': missing_ru,
                    'page_url_ru': page_url_ru,
                    'references_links_ru': references_links_ru,
                    'has_editor_ru': has_editor_ru,
                    'found_editors_ru': found_editors_ru,
                    # Українська локаль
                    'min_similarity_ua': round(min_sim_ua, 1),
                    'avg_similarity_ua': round(avg_sim_ua, 1),
                    'missing_count_ua': len(missing_ua),
                    'missing_fragments_ua': missing_ua,
                    'page_url_ua': page_url_ua,
                    'references_links_ua': references_links_ua,
                    'has_editor_ua': has_editor_ua,
                    'found_editors_ua': found_editors_ua,
                    # Загальні дані
                    'doc_preview': doc_text[:2000] if doc_text else "",
                    'page_preview_ru': page_text_ru[:2000] if page_text_ru else "",
                    'page_preview_ua': page_text_ua[:2000] if page_text_ua else "",
                    'doc_url': doc_url
                })

                progress_bar.progress((idx + 1) / len(df))
            
            st.session_state.results = results
            status_text.text("✅ Перевірка завершена!")
            progress_bar.empty()
    
    # Відображення результатів
    if 'results' in st.session_state:
        st.markdown("---")
        st.header("📋 Результати перевірки")
        
        results_df = pd.DataFrame(st.session_state.results)
        # Map results by original index to allow safe lookup after filtering
        results_map = {r['index']: r for r in st.session_state.results}
        display_df = df.copy()
        
        # Російська локаль
        display_df['Мін. схожість RU (%)'] = results_df['min_similarity_ru']
        display_df['Сер. схожість RU (%)'] = results_df['avg_similarity_ru']
        display_df['Проблемних фрагментів RU'] = results_df['missing_count_ru']
        
        # Українська локаль
        display_df['Мін. схожість UA (%)'] = results_df['min_similarity_ua']
        display_df['Сер. схожість UA (%)'] = results_df['avg_similarity_ua']
        display_df['Проблемних фрагментів UA'] = results_df['missing_count_ua']
        
        # Мінімальна схожість з обох локалей для фільтрації
        display_df['Мін. схожість (%)'] = display_df[['Мін. схожість RU (%)', 'Мін. схожість UA (%)']].min(axis=1)
        
        # Фільтрація
        if show_problems_only:
            display_df = display_df[display_df['Мін. схожість (%)'] < threshold]
        
        # Статистика для російської локалі
        st.markdown("### 🇷🇺 Російська локаль")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Всього записів", len(df))
        with col2:
            good_ru = len(display_df[display_df['Мін. схожість RU (%)'] >= threshold])
            st.metric("Добре", good_ru, delta=f"{good_ru/len(df)*100:.0f}%")
        with col3:
            warning_ru = len(display_df[(display_df['Мін. схожість RU (%)'] >= threshold-15) & 
                                       (display_df['Мін. схожість RU (%)'] < threshold)])
            st.metric("Увага", warning_ru, delta=f"{warning_ru/len(df)*100:.0f}%")
        with col4:
            problem_ru = len(display_df[display_df['Мін. схожість RU (%)'] < threshold-15])
            st.metric("Проблема", problem_ru, delta=f"{problem_ru/len(df)*100:.0f}%")
        
        # Статистика для української локалі
        st.markdown("### 🇺🇦 Українська локаль")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Всього записів", len(df))
        with col2:
            good_ua = len(display_df[display_df['Мін. схожість UA (%)'] >= threshold])
            st.metric("Добре", good_ua, delta=f"{good_ua/len(df)*100:.0f}%")
        with col3:
            warning_ua = len(display_df[(display_df['Мін. схожість UA (%)'] >= threshold-15) & 
                                      (display_df['Мін. схожість UA (%)'] < threshold)])
            st.metric("Увага", warning_ua, delta=f"{warning_ua/len(df)*100:.0f}%")
        with col4:
            problem_ua = len(display_df[display_df['Мін. схожість UA (%)'] < threshold-15])
            st.metric("Проблема", problem_ua, delta=f"{problem_ua/len(df)*100:.0f}%")
        
        st.markdown("---")
        
        # Таблиця результатів - окремі записи для кожної локалі
        for idx, row in display_df.iterrows():
            result = results_map.get(idx, {})
            doc_url = row.get('Посилання на відповідь анотацію, анкор', '')
            
            # Російська локаль - окремий запис
            min_sim_ru = row['Мін. схожість RU (%)']
            avg_sim_ru = row['Сер. схожість RU (%)']
            
            # Визначення статусу для російської локалі
            if min_sim_ru >= threshold:
                status_icon_ru = "🟢"
                status_color_ru = "green"
            elif min_sim_ru >= threshold - 15:
                status_icon_ru = "🟡"
                status_color_ru = "orange"
            else:
                status_icon_ru = "🔴"
                status_color_ru = "red"
            
            with st.expander(
                f"{status_icon_ru} 🇷🇺 **{row.get('Питання UKR', 'N/A')}** (RU) | Схожість: {min_sim_ru}% | Фрагментів: {row['Проблемних фрагментів RU']}"
            ):
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    st.markdown(f"**Мінімальна схожість:** :{status_color_ru}[{min_sim_ru}%]")
                    st.markdown(f"**Середня схожість:** {avg_sim_ru}%")
                    st.markdown(f"**Проблемних фрагментів:** {row['Проблемних фрагментів RU']}")
                
                with col2:
                    page_url_ru = result.get('page_url_ru', '')
                    if doc_url:
                        st.link_button("📄 Відкрити Google Doc", doc_url, use_container_width=True)
                    if page_url_ru:
                        st.link_button("🌐 Відкрити сторінку", page_url_ru, use_container_width=True)
                
                # Проблемні фрагменти (російська)
                missing_fragments_ru = result.get('missing_fragments_ru', [])
                if missing_fragments_ru:
                    st.markdown("### ⚠️ Ймовірно відсутні фрагменти:")
                    for i, fragment in enumerate(missing_fragments_ru, 1):
                        st.warning(f"**Фрагмент {i}** (схожість: {fragment['score']}%)\n\n{fragment['text']}")
                else:
                    st.success("✅ Всі фрагменти знайдено на сторінці!")
                
                # Список использованной литературы (російська)
                references_links_ru = result.get('references_links_ru', [])
                if references_links_ru:
                    st.markdown("### 📚 Список использованной литературы:")
                    for i, link in enumerate(references_links_ru, 1):
                        st.markdown(f"{i}. [{link}]({link})")
                else:
                    st.error("❌ Помилка: Блок 'Список использованной литературы' не знайдено або не містить посилань")
                
                # Перевірка наявності редактора (російська)
                has_editor_ru = result.get('has_editor_ru', False)
                found_editors_ru = result.get('found_editors_ru', [])
                if has_editor_ru and found_editors_ru:
                    editors_list_ru = ", ".join([f"**{editor}**" for editor in found_editors_ru])
                    st.success(f"✅ Редактор(и) знайдено: {editors_list_ru}")
                else:
                    st.error("❌ Помилка: Редактор не знайдено на сторінці (очікується: 'Щербаченко Юлія' або 'Севрюков Олександр Вікторович')")

                # Додаткова інформація — підвантажуємо повні тексти при відкритті (кешуються функціями)
                with st.expander("🔍 Детальна інформація"):
                    tab1, tab2 = st.tabs(["Текст з Docs", "Текст зі сторінки"])

                    with tab1:
                        if doc_url:
                            full_doc = get_doc_text(doc_url)
                            if full_doc:
                                display_text = full_doc if len(full_doc) <= 5000 else full_doc[:5000] + "..."
                                st.text_area(
                                    "Повний текст з Google Docs",
                                    display_text,
                                    height=300,
                                    key=f"doc_text_ru_{idx}"
                                )
                            else:
                                st.info("Не вдалося завантажити текст з документа")
                        else:
                            st.info("Посилання на документ не вказане")

                    with tab2:
                        page_url_ru = result.get('page_url_ru', '')
                        if page_url_ru:
                            full_page_ru = get_page_text(page_url_ru)
                            if full_page_ru:
                                display_text = full_page_ru if len(full_page_ru) <= 5000 else full_page_ru[:5000] + "..."
                                st.text_area(
                                    "Повний текст зі сторінки",
                                    display_text,
                                    height=300,
                                    key=f"page_text_ru_{idx}"
                                )
                            else:
                                st.info("Не вдалося завантажити текст зі сторінки")
                        else:
                            st.info("Посилання на сторінці не вказане")
            
            # Українська локаль - окремий запис
            min_sim_ua = row['Мін. схожість UA (%)']
            avg_sim_ua = row['Сер. схожість UA (%)']
            
            # Визначення статусу для української локалі
            if min_sim_ua >= threshold:
                status_icon_ua = "🟢"
                status_color_ua = "green"
            elif min_sim_ua >= threshold - 15:
                status_icon_ua = "🟡"
                status_color_ua = "orange"
            else:
                status_icon_ua = "🔴"
                status_color_ua = "red"
            
            with st.expander(
                f"{status_icon_ua} 🇺🇦 **{row.get('Питання UKR', 'N/A')}** (UA) | Схожість: {min_sim_ua}% | Фрагментів: {row['Проблемних фрагментів UA']}"
            ):
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    st.markdown(f"**Мінімальна схожість:** :{status_color_ua}[{min_sim_ua}%]")
                    st.markdown(f"**Середня схожість:** {avg_sim_ua}%")
                    st.markdown(f"**Проблемних фрагментів:** {row['Проблемних фрагментів UA']}")
                
                with col2:
                    page_url_ua = result.get('page_url_ua', '')
                    if doc_url:
                        st.link_button("📄 Відкрити Google Doc", doc_url, use_container_width=True)
                    if page_url_ua:
                        st.link_button("🌐 Відкрити сторінку", page_url_ua, use_container_width=True)
                
                # Проблемні фрагменти (українська)
                missing_fragments_ua = result.get('missing_fragments_ua', [])
                if missing_fragments_ua:
                    st.markdown("### ⚠️ Ймовірно відсутні фрагменти:")
                    for i, fragment in enumerate(missing_fragments_ua, 1):
                        st.warning(f"**Фрагмент {i}** (схожість: {fragment['score']}%)\n\n{fragment['text']}")
                else:
                    if min_sim_ua > 0:  # Перевірка була виконана
                        st.success("✅ Всі фрагменти знайдено на сторінці!")
                    else:
                        st.info("ℹ️ Перевірка не виконана (можливо, відсутня українська версія або фрагмент 'Перевод аннотации')")
                
                # Список использованной литературы (українська)
                references_links_ua = result.get('references_links_ua', [])
                if references_links_ua:
                    st.markdown("### 📚 Список использованной литературы:")
                    for i, link in enumerate(references_links_ua, 1):
                        st.markdown(f"{i}. [{link}]({link})")
                else:
                    if page_url_ua:
                        st.error("❌ Помилка: Блок 'Список использованной литературы' не знайдено або не містить посилань")
                
                # Перевірка наявності редактора (українська)
                has_editor_ua = result.get('has_editor_ua', False)
                found_editors_ua = result.get('found_editors_ua', [])
                if has_editor_ua and found_editors_ua:
                    editors_list_ua = ", ".join([f"**{editor}**" for editor in found_editors_ua])
                    st.success(f"✅ Редактор(и) знайдено: {editors_list_ua}")
                else:
                    if page_url_ua:
                        st.error("❌ Помилка: Редактор не знайдено на сторінці (очікується: 'Щербаченко Юлія' або 'Севрюков Олександр Вікторович')")

                # Додаткова інформація — підвантажуємо повні тексти при відкритті (кешуються функціями)
                with st.expander("🔍 Детальна інформація"):
                    tab1, tab2 = st.tabs(["Текст з Docs", "Текст зі сторінки"])

                    with tab1:
                        if doc_url:
                            full_doc = get_doc_text(doc_url)
                            if full_doc:
                                display_text = full_doc if len(full_doc) <= 5000 else full_doc[:5000] + "..."
                                st.text_area(
                                    "Повний текст з Google Docs",
                                    display_text,
                                    height=300,
                                    key=f"doc_text_ua_{idx}"
                                )
                            else:
                                st.info("Не вдалося завантажити текст з документа")
                        else:
                            st.info("Посилання на документ не вказане")

                    with tab2:
                        page_url_ua = result.get('page_url_ua', '')
                        if page_url_ua:
                            full_page_ua = get_page_text(page_url_ua)
                            if full_page_ua:
                                display_text = full_page_ua if len(full_page_ua) <= 5000 else full_page_ua[:5000] + "..."
                                st.text_area(
                                    "Повний текст зі сторінки",
                                    display_text,
                                    height=300,
                                    key=f"page_text_ua_{idx}"
                                )
                            else:
                                st.info("Не вдалося завантажити текст зі сторінки")
                        else:
                            st.info("Посилання на сторінці не вказане")

else:
    st.error("❌ Не вдалося завантажити дані. Перевірте підключення до Google Sheets.")

# Футер
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray;'>
    Розроблено для перевірки розміщення текстів з Google Docs на сторінках сайту
    </div>
    """,
    unsafe_allow_html=True
)