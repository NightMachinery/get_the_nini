#!/usr/bin/env python3
"""
Ninisite Post Scraper - Fetches all pages of a Ninisite discussion and formats in org-mode
"""

import requests
from bs4 import BeautifulSoup
import re
import sys
import time
import argparse
from urllib.parse import urljoin, urlparse, parse_qs
from typing import List, Dict, Optional
import html
from datetime import datetime
import pytz
try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False


def log_message(message: str):
    """Log message to stderr to avoid interfering with stdout output"""
    print(message, file=sys.stderr)


class OrgWriter:
    """Writer class for streaming org-mode output to file or stdout"""
    
    def __init__(self, output_file: str = None):
        self.output_file = output_file
        self.file_handle = None
        self._setup_output()
    
    def _setup_output(self):
        """Setup output destination"""
        if self.output_file == '-':
            self.file_handle = sys.stdout
        elif self.output_file:
            self.file_handle = open(self.output_file, 'w', encoding='utf-8')
        else:
            # Will be set later when we know the topic ID
            self.file_handle = None
    
    def set_auto_filename(self, topic_id: str):
        """Set auto-generated filename based on topic ID"""
        if not self.file_handle:
            self.output_file = f"ninisite_topic_{topic_id}.org"
            self.file_handle = open(self.output_file, 'w', encoding='utf-8')
    
    def write(self, content: str):
        """Write content to output"""
        if self.file_handle:
            self.file_handle.write(content)
            self.file_handle.flush()  # Ensure streaming
    
    def writeln(self, content: str = ""):
        """Write content with newline"""
        self.write(content + "\n")
    
    def close(self):
        """Close file handle if it's not stdout"""
        if self.file_handle and self.file_handle != sys.stdout:
            self.file_handle.close()
            if self.output_file and self.output_file != '-':
                log_message(f"Successfully saved to {self.output_file}")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class NinisiteScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        
    def fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch a single page and return BeautifulSoup object"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except Exception as e:
            log_message(f"Error fetching {url}: {e}")
            return None
    
    def detect_total_pages(self, base_url: str) -> int:
        """Detect total number of pages by checking pagination"""
        soup = self.fetch_page(base_url)
        if not soup:
            return 1
            
        pagination = soup.find('ul', class_='pagination')
        if not pagination:
            return 1
            
        # Look for page numbers in pagination
        page_links = pagination.find_all('a')
        max_page = 1
        
        for link in page_links:
            href = link.get('href', '')
            # Extract page number from URL
            if 'page=' in href:
                try:
                    page_num = int(href.split('page=')[1].split('&')[0])
                    max_page = max(max_page, page_num)
                except (ValueError, IndexError):
                    continue
                    
            # Also check link text for page numbers
            text = link.get_text().strip()
            if text.isdigit():
                max_page = max(max_page, int(text))
                
        return max_page

    def get_all_pages(self, base_url: str) -> List[BeautifulSoup]:
        """Get all pages of a discussion thread"""
        # Detect total pages for progress tracking
        total_pages = self.detect_total_pages(base_url)
        
        # Set up progress bar if tqdm is available and stdout is a tty
        use_progress = HAS_TQDM and sys.stdout.isatty()
        if use_progress:
            pbar = tqdm(total=total_pages, desc="Fetching pages", unit="page")
        
        pages = []
        current_url = base_url
        page_num = 1
        
        while current_url:
            if not use_progress:
                log_message(f"Fetching page {page_num}/{total_pages}: {current_url}")
            
            soup = self.fetch_page(current_url)
            if not soup:
                break
                
            pages.append(soup)
            
            if use_progress:
                pbar.update(1)
            
            # Find next page URL
            pagination = soup.find('ul', class_='pagination')
            if pagination:
                # Look for next page link (< symbol)
                next_links = pagination.find_all('a', title='Next page')
                if next_links:
                    next_href = next_links[0].get('href')
                    if next_href and next_href != '#':
                        current_url = urljoin(base_url, next_href)
                        page_num += 1
                    else:
                        current_url = None
                else:
                    current_url = None
            else:
                current_url = None
                
            # Be respectful to the server
            time.sleep(1)
            
        if use_progress:
            pbar.close()
            
        return pages
    
    def extract_topic_metadata(self, soup: BeautifulSoup) -> Dict:
        """Extract metadata from the main topic"""
        metadata = {}
        
        # Topic title
        title_elem = soup.find('h1', class_='topic-title')
        if title_elem:
            title_link = title_elem.find('a')
            metadata['title'] = title_link.get_text().strip() if title_link else title_elem.get_text().strip()
        
        # Main topic article
        topic_article = soup.find('article', id='topic')
        if topic_article:
            # Author info
            author_elem = topic_article.find('span', itemprop='name')
            if author_elem:
                metadata['author'] = author_elem.get_text().strip()
            
            # Date
            date_elem = topic_article.find('meta', itemprop='datepublished')
            if date_elem:
                metadata['date'] = date_elem.get('content')
            
            # View count
            view_elem = topic_article.find('meta', itemprop='userInteractionCount')
            if view_elem:
                metadata['views'] = view_elem.get('content')
        
        # Breadcrumb for category
        breadcrumb = soup.find('ol', itemtype='http://schema.org/BreadcrumbList')
        if breadcrumb:
            categories = []
            for item in breadcrumb.find_all('li', itemprop='itemListElement'):
                name_elem = item.find('span', itemprop='name')
                if name_elem:
                    categories.append(name_elem.get_text().strip())
            metadata['categories'] = categories[1:]  # Skip the first "تبادل نظر"
        
        return metadata
    
    def extract_posts(self, pages: List[BeautifulSoup]) -> List[Dict]:
        """Extract all posts from all pages"""
        all_posts = []
        
        for page_num, soup in enumerate(pages, 1):
            # Main topic (only on first page)
            if page_num == 1:
                topic_article = soup.find('article', id='topic')
                if topic_article:
                    post = self.extract_post_data(topic_article, is_main_topic=True)
                    if post:
                        post['page'] = page_num
                        all_posts.append(post)
            
            # Reply posts
            reply_articles = soup.find_all('article', id=re.compile(r'post-\d+'))
            for article in reply_articles:
                # Skip ads and special content
                if 'forum-native-ad' in article.get('class', []):
                    continue
                    
                post = self.extract_post_data(article, is_main_topic=False)
                if post:
                    post['page'] = page_num
                    all_posts.append(post)
        
        return all_posts
    
    def extract_post_data(self, article, is_main_topic=False) -> Optional[Dict]:
        """Extract data from a single post article"""
        post = {}
        
        # Post ID
        post_id = article.get('id')
        if post_id:
            post['id'] = post_id
        
        # Author info
        author_elem = article.find('span', itemprop='name')
        if author_elem:
            post['author'] = author_elem.get_text().strip()
        
        # Author link for profile
        author_link = article.find('a', itemprop='url')
        if author_link:
            post['author_profile'] = author_link.get('href')
        
        # Join date and post count
        reg_date_elem = article.find('div', class_='reg-date')
        if reg_date_elem:
            post['author_join_date'] = reg_date_elem.get_text().strip()
        
        post_count_elem = article.find('div', class_='post-count')
        if post_count_elem:
            post['author_post_count'] = post_count_elem.get_text().strip()
        
        # Post date/time
        date_elem = article.find('meta', itemprop='datepublished')
        if date_elem:
            post['date'] = date_elem.get('content')
        
        # Post content
        message_elem = article.find('div', class_='post-message')
        if message_elem:
            # Clean up the content
            content = message_elem.get_text().strip()
            post['content'] = content
            
            # Also get HTML for potential formatting
            post['content_html'] = str(message_elem)
        
        # Quote/reply reference
        quote_elem = article.find('div', class_='topic-post__quotation')
        if quote_elem:
            reply_msg = quote_elem.find('div', class_='reply-message')
            if reply_msg:
                post['quoted_content'] = reply_msg.get_text().strip()
                # Get the referenced post ID
                ref_id = reply_msg.get('data-id')
                if ref_id:
                    post['reply_to_id'] = ref_id
        
        # Like count
        like_elem = article.find('a', class_='like-count')
        if like_elem:
            like_span = like_elem.find('span')
            if like_span:
                post['likes'] = like_span.get_text().strip()
        
        # Signature
        signature_elem = article.find('div', class_='topic-post__signature')
        if signature_elem:
            post['signature'] = signature_elem.get_text().strip()
        
        post['is_main_topic'] = is_main_topic
        
        return post if post.get('content') or post.get('is_main_topic') else None
    
    def parse_date_to_jalali(self, date_str: str) -> str:
        """Convert date string to Jalali format in Tehran timezone"""
        try:
            # Parse the date string like "7/4/2023 8:02:48 AM"
            dt = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
            
            # Assume it's already in Tehran timezone (Asia/Tehran)
            tehran_tz = pytz.timezone('Asia/Tehran')
            dt_tehran = tehran_tz.localize(dt)
            
            # Convert to Jalali (Persian) calendar
            # For simplicity, we'll use a basic conversion formula
            # This is approximate - for exact conversion you'd need a proper Jalali library
            year = dt_tehran.year
            month = dt_tehran.month
            day = dt_tehran.day
            hour = dt_tehran.hour
            minute = dt_tehran.minute
            
            # Simple Gregorian to Jalali conversion (approximate)
            j_year = year - 621 if month < 3 or (month == 3 and day < 21) else year - 620
            
            return f"jalali:{j_year:04d}/{month:02d}/{day:02d}/{hour:02d}:{minute:02d}"
        except:
            # Fallback to original date if parsing fails
            return f"jalali:{date_str}"

    def clean_author_info(self, author_join_date: str, author_post_count: str) -> tuple:
        """Clean and extract author info"""
        join_date = ""
        post_count = ""
        
        if author_join_date:
            # Extract just the date part from "عضویت: 1401/06/16"
            match = re.search(r'(\d{4}/\d{2}/\d{2})', author_join_date)
            if match:
                join_date = match.group(1)
        
        if author_post_count:
            # Extract just the number from "تعداد پست: 674"
            match = re.search(r'(\d+)', author_post_count)
            if match:
                post_count = match.group(1)
        
        return join_date, post_count

    def format_author_name(self, author: str) -> str:
        """Add bidi marks around author names that start with Persian characters"""
        if not author:
            return author
            
        # Check if the first character is Persian/Arabic
        # Persian/Arabic Unicode ranges: 0x0600-0x06FF, 0x0750-0x077F, 0xFB50-0xFDFF, 0xFE70-0xFEFF
        first_char = author[0]
        is_persian = (
            '\u0600' <= first_char <= '\u06FF' or  # Arabic
            '\u0750' <= first_char <= '\u077F' or  # Arabic Supplement
            '\uFB50' <= first_char <= '\uFDFF' or  # Arabic Presentation Forms-A
            '\uFE70' <= first_char <= '\uFEFF'     # Arabic Presentation Forms-B
        )
        
        if is_persian:
            # Add Right-to-Left Isolate (RLI) and Pop Directional Isolate (PDI) marks
            return f'\u2067{author}\u2069'  # RLI + author + PDI
        else:
            return author

    def extract_topic_id(self, url: str) -> str:
        """Extract topic ID from URL"""
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split('/')
        
        # Look for numeric topic ID in path
        for part in path_parts:
            if part.isdigit():
                return part
        
        return 'unknown'

    def format_org_mode_streaming(self, metadata: Dict, posts: List[Dict], base_url: str, writer: OrgWriter, paginate: bool = True):
        """Format the scraped data as org-mode with streaming output"""
        # File title
        title = metadata.get('title', 'Ninisite Post')
        writer.writeln(f"#+TITLE: {title}")
        writer.writeln()
        
        # Calculate additional metadata
        unique_authors = len(set(post.get('author', 'Unknown') for post in posts))
        num_pages = max(post.get('page', 1) for post in posts) if posts else 1
        scrape_time = self.parse_date_to_jalali(datetime.now().strftime("%m/%d/%Y %I:%M:%S %p"))
        topic_id = self.extract_topic_id(base_url)
        
        # Set auto filename if needed
        writer.set_auto_filename(topic_id)
        
        # Main header
        writer.writeln(f"* {title}")
        writer.writeln(":PROPERTIES:")
        writer.writeln(f":TOPIC_ID: {topic_id}")
        writer.writeln(f":ORIGINAL_URL: {base_url}")
        writer.writeln(f":SCRAPE_DATE: {scrape_time}")
        writer.writeln(f":TOTAL_PAGES: {num_pages}")
        writer.writeln(f":UNIQUE_AUTHORS: {unique_authors}")
        if metadata.get('author'):
            writer.writeln(f":AUTHOR: {metadata['author']}")
        if metadata.get('date'):
            writer.writeln(f":DATE: {metadata['date']}")
        if metadata.get('views'):
            writer.writeln(f":VIEWS: {metadata['views']}")
        if metadata.get('categories'):
            writer.writeln(f":CATEGORIES: {' > '.join(metadata['categories'])}")
        writer.writeln(f":TOTAL_POSTS: {len(posts)}")
        writer.writeln(":END:")
        writer.writeln()
        
        if paginate:
            # Group posts by page
            posts_by_page = {}
            for post in posts:
                page_num = post.get('page', 1)
                if page_num not in posts_by_page:
                    posts_by_page[page_num] = []
                posts_by_page[page_num].append(post)
            
            # Process each page
            for page_num in sorted(posts_by_page.keys()):
                page_posts = posts_by_page[page_num]
                
                # Create page URL
                if page_num == 1:
                    page_url = base_url
                else:
                    # Add or modify page parameter
                    if '?' in base_url:
                        if 'page=' in base_url:
                            # Replace existing page parameter
                            page_url = re.sub(r'page=\d+', f'page={page_num}', base_url)
                        else:
                            # Add page parameter to existing query string
                            page_url = f"{base_url}&page={page_num}"
                    else:
                        # Add page parameter as first query parameter
                        page_url = f"{base_url}?page={page_num}"
                
                # Page heading
                if page_num == 1:
                    writer.writeln(f"** [[{page_url}][1st Page]]")
                elif page_num == 2:
                    writer.writeln(f"** [[{page_url}][2nd Page]]")
                elif page_num == 3:
                    writer.writeln(f"** [[{page_url}][3rd Page]]")
                else:
                    writer.writeln(f"** [[{page_url}][{page_num}th Page]]")
                
                # Process posts for this page
                for post in page_posts:
                    self._format_post_streaming(post, writer, heading_level="***")
        else:
            # Non-paginated: process all posts directly under main heading
            for post in posts:
                self._format_post_streaming(post, writer, heading_level="**")

    def format_org_mode(self, metadata: Dict, posts: List[Dict], base_url: str, paginate: bool = True) -> str:
        """Format the scraped data as org-mode (non-streaming version)"""
        org_content = []
        
        # File title
        title = metadata.get('title', 'Ninisite Post')
        org_content.append(f"#+TITLE: {title}")
        org_content.append("")
        
        # Calculate additional metadata
        unique_authors = len(set(post.get('author', 'Unknown') for post in posts))
        num_pages = max(post.get('page', 1) for post in posts) if posts else 1
        scrape_time = self.parse_date_to_jalali(datetime.now().strftime("%m/%d/%Y %I:%M:%S %p"))
        topic_id = self.extract_topic_id(base_url)
        
        # Main header
        org_content.append(f"* {title}")
        org_content.append(":PROPERTIES:")
        org_content.append(f":TOPIC_ID: {topic_id}")
        org_content.append(f":ORIGINAL_URL: {base_url}")
        org_content.append(f":SCRAPE_DATE: {scrape_time}")
        org_content.append(f":TOTAL_PAGES: {num_pages}")
        org_content.append(f":UNIQUE_AUTHORS: {unique_authors}")
        if metadata.get('author'):
            org_content.append(f":AUTHOR: {metadata['author']}")
        if metadata.get('date'):
            org_content.append(f":DATE: {metadata['date']}")
        if metadata.get('views'):
            org_content.append(f":VIEWS: {metadata['views']}")
        if metadata.get('categories'):
            org_content.append(f":CATEGORIES: {' > '.join(metadata['categories'])}")
        org_content.append(f":TOTAL_POSTS: {len(posts)}")
        org_content.append(":END:")
        org_content.append("")
        
        if paginate:
            # Group posts by page
            posts_by_page = {}
            for post in posts:
                page_num = post.get('page', 1)
                if page_num not in posts_by_page:
                    posts_by_page[page_num] = []
                posts_by_page[page_num].append(post)
            
            # Process each page
            for page_num in sorted(posts_by_page.keys()):
                page_posts = posts_by_page[page_num]
                
                # Create page URL
                if page_num == 1:
                    page_url = base_url
                else:
                    # Add or modify page parameter
                    if '?' in base_url:
                        if 'page=' in base_url:
                            # Replace existing page parameter
                            page_url = re.sub(r'page=\d+', f'page={page_num}', base_url)
                        else:
                            # Add page parameter to existing query string
                            page_url = f"{base_url}&page={page_num}"
                    else:
                        # Add page parameter as first query parameter
                        page_url = f"{base_url}?page={page_num}"
                
                # Page heading
                if page_num == 1:
                    org_content.append(f"** [[{page_url}][1st Page]]")
                elif page_num == 2:
                    org_content.append(f"** [[{page_url}][2nd Page]]")
                elif page_num == 3:
                    org_content.append(f"** [[{page_url}][3rd Page]]")
                else:
                    org_content.append(f"** [[{page_url}][{page_num}th Page]]")
                
                # Process posts for this page
                for post in page_posts:
                    self._format_post(post, org_content, heading_level="***")
        else:
            # Non-paginated: process all posts directly under main heading
            for post in posts:
                self._format_post(post, org_content, heading_level="**")
        
        return '\n'.join(org_content)
    
    def _format_post_streaming(self, post: Dict, writer: OrgWriter, heading_level: str = "**"):
        """Format a single post and write to streaming output"""
        # Format heading with @likes/{count} author (join_date, post_count posts) [date]
        likes = post.get('likes', '0')
        author = post.get('author', 'Unknown')
        author_formatted = self.format_author_name(author)
        date_formatted = self.parse_date_to_jalali(post.get('date', ''))
        
        join_date, post_count = self.clean_author_info(
            post.get('author_join_date', ''), 
            post.get('author_post_count', '')
        )
        
        author_info = f"({join_date}, {post_count} posts)" if join_date and post_count else ""
        
        # Only include likes if > 0, and use @likes/{count} format
        likes_str = ""
        if likes and likes != '0' and int(likes) > 0:
            likes_str = f"@likes/{likes} "
        
        heading = f"{heading_level} {likes_str}{author_formatted} {author_info} [{date_formatted}]"
        
        writer.writeln(heading)
        
        # Post properties
        writer.writeln(":PROPERTIES:")
        if post.get('id'):
            # Extract post ID for CUSTOM_ID (remove 'post-' prefix if present)
            custom_id = post['id'].replace('post-', '') if post['id'].startswith('post-') else post['id']
            writer.writeln(f":CUSTOM_ID: {custom_id}")
        if post.get('author'):
            writer.writeln(f":AUTHOR: {post['author']}")
        if post.get('date'):
            writer.writeln(f":DATE: {post['date']}")
        if post.get('author_join_date'):
            writer.writeln(f":AUTHOR_JOIN_DATE: {post['author_join_date']}")
        if post.get('author_post_count'):
            writer.writeln(f":AUTHOR_POST_COUNT: {post['author_post_count']}")
        if post.get('likes'):
            writer.writeln(f":LIKES: {post['likes']}")
        if post.get('page'):
            writer.writeln(f":PAGE: {post['page']}")
        if post.get('reply_to_id'):
            writer.writeln(f":REPLY_TO_ID: {post['reply_to_id']}")
        writer.writeln(":END:")
        
        # Reply link (if replying to someone)
        if post.get('reply_to_id'):
            reply_id = post['reply_to_id'].replace('post-', '') if post['reply_to_id'].startswith('post-') else post['reply_to_id']
            writer.writeln(f"- [[#{reply_id}][In Reply To]]")
            writer.writeln()
        
        # Quoted content (if replying to someone)
        if post.get('quoted_content'):
            writer.writeln("#+begin_quote")
            quoted_lines = post['quoted_content'].split('\n')
            for line in quoted_lines:
                if line.strip():
                    writer.writeln(line.strip())
            writer.writeln("#+end_quote")
            writer.writeln()
        
        # Main content
        if post.get('content'):
            content_lines = post['content'].split('\n')
            for line in content_lines:
                if line.strip():
                    writer.writeln(line.strip())
            writer.writeln()
        
        # Signature
        if post.get('signature'):
            signature_level = "***" if heading_level == "**" else "****"
            writer.writeln(f"{signature_level} Signature:")
            sig_lines = post['signature'].split('\n')
            for line in sig_lines:
                if line.strip():
                    writer.writeln(line.strip())
            writer.writeln()
    
    def _format_post(self, post: Dict, org_content: List[str], heading_level: str = "**"):
        """Format a single post and append to org_content"""
        # Format heading with @likes/{count} author (join_date, post_count posts) [date]
        likes = post.get('likes', '0')
        author = post.get('author', 'Unknown')
        author_formatted = self.format_author_name(author)
        date_formatted = self.parse_date_to_jalali(post.get('date', ''))
        
        join_date, post_count = self.clean_author_info(
            post.get('author_join_date', ''), 
            post.get('author_post_count', '')
        )
        
        author_info = f"({join_date}, {post_count} posts)" if join_date and post_count else ""
        
        # Only include likes if > 0, and use @likes/{count} format
        likes_str = ""
        if likes and likes != '0' and int(likes) > 0:
            likes_str = f"@likes/{likes} "
        
        heading = f"{heading_level} {likes_str}{author_formatted} {author_info} [{date_formatted}]"
        
        org_content.append(heading)
        
        # Post properties
        org_content.append(":PROPERTIES:")
        if post.get('id'):
            # Extract post ID for CUSTOM_ID (remove 'post-' prefix if present)
            custom_id = post['id'].replace('post-', '') if post['id'].startswith('post-') else post['id']
            org_content.append(f":CUSTOM_ID: {custom_id}")
        if post.get('author'):
            org_content.append(f":AUTHOR: {post['author']}")
        if post.get('date'):
            org_content.append(f":DATE: {post['date']}")
        if post.get('author_join_date'):
            org_content.append(f":AUTHOR_JOIN_DATE: {post['author_join_date']}")
        if post.get('author_post_count'):
            org_content.append(f":AUTHOR_POST_COUNT: {post['author_post_count']}")
        if post.get('likes'):
            org_content.append(f":LIKES: {post['likes']}")
        if post.get('page'):
            org_content.append(f":PAGE: {post['page']}")
        if post.get('reply_to_id'):
            org_content.append(f":REPLY_TO_ID: {post['reply_to_id']}")
        org_content.append(":END:")
        
        # Reply link (if replying to someone)
        if post.get('reply_to_id'):
            reply_id = post['reply_to_id'].replace('post-', '') if post['reply_to_id'].startswith('post-') else post['reply_to_id']
            org_content.append(f"- [[#{reply_id}][In Reply To]]")
            org_content.append("")
        
        # Quoted content (if replying to someone)
        if post.get('quoted_content'):
            org_content.append("#+begin_quote")
            quoted_lines = post['quoted_content'].split('\n')
            for line in quoted_lines:
                if line.strip():
                    org_content.append(line.strip())
            org_content.append("#+end_quote")
            org_content.append("")
        
        # Main content
        if post.get('content'):
            content_lines = post['content'].split('\n')
            for line in content_lines:
                if line.strip():
                    org_content.append(line.strip())
            org_content.append("")
        
        # Signature
        if post.get('signature'):
            signature_level = "***" if heading_level == "**" else "****"
            org_content.append(f"{signature_level} Signature:")
            sig_lines = post['signature'].split('\n')
            for line in sig_lines:
                if line.strip():
                    org_content.append(line.strip())
            org_content.append("")
    
    def scrape_discussion_streaming(self, url: str, writer: OrgWriter, paginate: bool = True):
        """Main method to scrape a discussion and stream org-mode formatted content"""
        log_message(f"Starting to scrape: {url}")
        
        # Get all pages
        pages = self.get_all_pages(url)
        if not pages:
            raise Exception("Could not fetch any pages")
        
        log_message(f"Found {len(pages)} pages")
        
        # Extract metadata from first page
        metadata = self.extract_topic_metadata(pages[0])
        
        # Extract all posts
        posts = self.extract_posts(pages)
        log_message(f"Extracted {len(posts)} posts")
        
        # Format as org-mode with streaming
        self.format_org_mode_streaming(metadata, posts, url, writer, paginate)

    def scrape_discussion(self, url: str, paginate: bool = True) -> str:
        """Main method to scrape a discussion and return org-mode formatted content"""
        log_message(f"Starting to scrape: {url}")
        
        # Get all pages
        pages = self.get_all_pages(url)
        if not pages:
            raise Exception("Could not fetch any pages")
        
        log_message(f"Found {len(pages)} pages")
        
        # Extract metadata from first page
        metadata = self.extract_topic_metadata(pages[0])
        
        # Extract all posts
        posts = self.extract_posts(pages)
        log_message(f"Extracted {len(posts)} posts")
        
        # Format as org-mode
        return self.format_org_mode(metadata, posts, url, paginate)


def main():
    parser = argparse.ArgumentParser(description='Scrape Ninisite discussion posts and format as org-mode')
    parser.add_argument('url', nargs='*', help='Ninisite discussion URL(s) to scrape')
    parser.add_argument('--paginate', action=argparse.BooleanOptionalAction, default=True,
                        help='Organize posts under page headings (default: True)')
    parser.add_argument('--streaming', action=argparse.BooleanOptionalAction, default=True,
                        help='Stream output as posts are processed (default: True)')
    parser.add_argument('-o', '--out', 
                        help='Output file (use "-" for stdout, default: auto-generate from topic ID)')
    
    args = parser.parse_args()
    
    # Handle URL parsing with -- support
    if not args.url:
        parser.error("At least one URL is required")
    
    if len(args.url) > 1:
        parser.error("Only one URL is supported at this time")
    
    url = args.url[0]
    
    scraper = NinisiteScraper()
    try:
        if args.streaming:
            # Streaming mode
            with OrgWriter(args.out) as writer:
                scraper.scrape_discussion_streaming(url, writer, args.paginate)
        else:
            # Non-streaming mode (legacy)
            org_content = scraper.scrape_discussion(url, args.paginate)
            
            # Handle output
            if args.out == '-':
                print(org_content)
            elif args.out:
                with open(args.out, 'w', encoding='utf-8') as f:
                    f.write(org_content)
                log_message(f"Successfully saved to {args.out}")
            else:
                topic_id = scraper.extract_topic_id(url)
                output_file = f"ninisite_topic_{topic_id}.org"
                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(org_content)
                log_message(f"Successfully saved to {output_file}")
        
    except Exception as e:
        log_message(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()