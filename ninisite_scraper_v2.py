#!/usr/bin/env python3
"""
Niniesite Thread Scraper

Fetches all pages of a Niniesite discussion thread and converts to markdown.
Includes metadata extraction for usernames, timestamps, and other relevant data.
"""

import requests
import re
import json
import subprocess
import sys
from urllib.parse import urljoin, urlparse, parse_qs, unquote
from bs4 import BeautifulSoup
from datetime import datetime
import argparse
import tempfile
import os


class NiniesiteScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.posts = []
        self.metadata = {}

    def get_thread_info(self, url):
        """Extract thread ID and title from URL"""
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        
        if len(path_parts) >= 4 and path_parts[2] == 'topic':
            thread_id = path_parts[3]
            thread_title = unquote(path_parts[4]) if len(path_parts) > 4 else "untitled"
            return thread_id, thread_title
        
        raise ValueError("Invalid Niniesite URL format")

    def fetch_page(self, url, page_num=1):
        """Fetch a specific page of the thread"""
        if page_num > 1:
            if '?' in url:
                url += f'&page={page_num}'
            else:
                url += f'?page={page_num}'
        
        response = self.session.get(url)
        response.raise_for_status()
        return BeautifulSoup(response.content, 'html.parser')

    def extract_posts_from_page(self, soup):
        """Extract posts and metadata from a page"""
        posts = []
        
        # Simple approach: find all text blocks and filter intelligently
        all_divs = soup.find_all('div')
        
        for div in all_divs:
            text = div.get_text().strip()
            
            # Skip empty or very short text
            if len(text) < 50:
                continue
                
            # Skip obvious navigation/UI elements
            if any(ui_term in text.lower() for ui_term in ['login', 'register', 'menu', 'navigation', 'footer', 'header']):
                continue
                
            # Skip if it contains too many navigation terms
            nav_count = sum(1 for term in ['تبادل نظر', 'ثبت نام', 'مجله', 'فروشگاه', 'کانون'] if term in text)
            if nav_count > 3:
                continue
            
            # Must have Persian text
            if not re.search(r'[\u0600-\u06FF]', text):
                continue
                
            # Extract data and validate
            post_data = self.extract_post_data(div)
            if post_data and self.is_valid_post(post_data):
                posts.append(post_data)
        
        return posts

    def extract_post_data(self, post_elem):
        """Extract individual post data"""
        post_data = {
            'content': '',
            'username': '',
            'timestamp': '',
            'post_number': '',
            'user_info': {}
        }
        
        # Extract username - look for username links or class
        username_elem = post_elem.find('a', class_='username') or post_elem.find('a', href=re.compile(r'/user/'))
        if username_elem:
            post_data['username'] = username_elem.get_text().strip()
        
        # Extract post content - look for post-content div or main text
        content_elem = post_elem.find('div', class_=lambda x: x and 'post-content' in x.lower())
        if content_elem:
            # Get text from p tags within post-content
            content_p = content_elem.find('p')
            if content_p:
                content = content_p.get_text().strip()
            else:
                content = content_elem.get_text().strip()
        else:
            # Fallback: get all text but exclude user info sections
            content = post_elem.get_text().strip()
            # Remove user info patterns
            content = re.sub(r'عضویت:\s*\d{4}/\d{2}/\d{2}', '', content)
            content = re.sub(r'تعداد پست:\s*\d+', '', content)
        
        # Extract timestamp
        timestamp_elem = post_elem.find('span', class_='timestamp')
        if timestamp_elem:
            post_data['timestamp'] = timestamp_elem.get_text().strip()
        else:
            # Look for timestamp pattern in text
            timestamp_match = re.search(r'\d{4}/\d{2}/\d{2}\s*\|?\s*\d{2}:\d{2}', post_elem.get_text())
            if timestamp_match:
                post_data['timestamp'] = timestamp_match.group(0)
        
        # Clean content
        content = self.clean_content(content)
        post_data['content'] = content
        
        return post_data if content else None

    def clean_content(self, content):
        """Clean up content by removing formatting artifacts and noise"""
        if not content:
            return ""
        
        # Remove excessive whitespace and line breaks
        content = re.sub(r'\s+', ' ', content)
        
        # Remove backslashes used in markdown escaping
        content = re.sub(r'\\+', '', content)
        
        # Remove standalone punctuation and symbols
        content = re.sub(r'\s*[|:]+\s*', ' ', content)
        
        # Remove metadata patterns like "تعداد پست: 123" or "عضویت: date"
        content = re.sub(r'(تعداد پست|عضویت|امتیاز):\s*[\d/]+', '', content)
        
        # Remove navigation/menu terms
        nav_terms = ['تبادل نظر', 'ثبت نام', 'مجله', 'فروشگاه', 'بارداری', 'مشاورین', 'کانون', 'دسته بندی']
        for term in nav_terms:
            content = content.replace(term, '')
        
        # Remove standalone dates without context
        content = re.sub(r'^\s*\d{4}/\d{2}/\d{2}\s*$', '', content)
        
        # Remove very short content that's likely metadata
        content = content.strip()
        if len(content) < 30:  # Increased minimum length
            return ""
            
        return content

    def is_valid_post(self, post_data):
        """Determine if this is a valid user post vs metadata/UI element"""
        if not post_data or not post_data.get('content'):
            return False
            
        content = post_data['content']
        
        # Skip if content is too short
        if len(content) < 30:
            return False
            
        # Skip if content is just metadata
        metadata_patterns = [
            r'^(تعداد پست|عضویت|امتیاز):\s*[\d/]+$',
            r'^\d{4}/\d{2}/\d{2}\s*$',
            r'^[|:\s]+$',
            r'^(مدیر|استارتر)$'
        ]
        
        for pattern in metadata_patterns:
            if re.match(pattern, content.strip()):
                return False
        
        # Skip navigation-heavy content
        nav_ratio = sum(1 for term in ['تبادل نظر', 'ثبت نام', 'مجله', 'فروشگاه'] if term in content)
        if nav_ratio > 2:  # Too many navigation terms
            return False
        
        # Must have Persian text and be conversational
        has_persian = bool(re.search(r'[\u0600-\u06FF]', content))
        word_count = len(content.split())
        
        return has_persian and word_count >= 5 and word_count <= 200

    def get_total_pages(self, soup):
        """Determine total number of pages"""
        page_links = soup.find_all('a', href=re.compile(r'page=\d+'))
        if page_links:
            page_numbers = []
            for link in page_links:
                href = link.get('href', '')
                match = re.search(r'page=(\d+)', href)
                if match:
                    page_numbers.append(int(match.group(1)))
            return max(page_numbers) if page_numbers else 1
        
        # Look for pagination info in text
        pagination_text = soup.get_text()
        match = re.search(r'(\d+)\s*صفحه|page\s*(\d+)', pagination_text, re.IGNORECASE)
        if match:
            return int(match.group(1) or match.group(2))
        
        return 1

    def scrape_thread(self, url):
        """Scrape entire thread from all pages"""
        print(f"Fetching thread: {url}")
        
        thread_id, thread_title = self.get_thread_info(url)
        self.metadata = {
            'thread_id': thread_id,
            'thread_title': thread_title,
            'source_url': url,
            'scraped_at': datetime.now().isoformat(),
            'total_pages': 0,
            'total_posts': 0
        }
        
        # Fetch first page to determine total pages
        soup = self.fetch_page(url, 1)
        total_pages = self.get_total_pages(soup)
        self.metadata['total_pages'] = total_pages
        
        print(f"Found {total_pages} pages to scrape")
        
        # Scrape all pages
        all_posts = []
        for page_num in range(1, total_pages + 1):
            print(f"Scraping page {page_num}/{total_pages}")
            soup = self.fetch_page(url, page_num)
            posts = self.extract_posts_from_page(soup)
            all_posts.extend(posts)
        
        # Remove duplicates while preserving order
        seen_content = set()
        unique_posts = []
        for post in all_posts:
            content_hash = hash(post['content'][:100])  # Use first 100 chars as identifier
            if content_hash not in seen_content:
                seen_content.add(content_hash)
                unique_posts.append(post)
        
        self.posts = unique_posts
        self.metadata['total_posts'] = len(self.posts)
        print(f"Extracted {len(self.posts)} unique posts")
        
        return self.posts, self.metadata

    def posts_to_html(self):
        """Convert posts to HTML format"""
        html_content = f"""
        <html>
        <head>
            <meta charset="UTF-8">
            <title>{self.metadata.get('thread_title', 'Niniesite Thread')}</title>
        </head>
        <body>
            <h1>{self.metadata.get('thread_title', 'Niniesite Thread')}</h1>
            <div class="metadata">
                <p><strong>Thread ID:</strong> {self.metadata.get('thread_id', 'Unknown')}</p>
                <p><strong>Source URL:</strong> <a href="{self.metadata.get('source_url', '')}">{self.metadata.get('source_url', '')}</a></p>
                <p><strong>Total Posts:</strong> {self.metadata.get('total_posts', 0)}</p>
                <p><strong>Total Pages:</strong> {self.metadata.get('total_pages', 0)}</p>
                <p><strong>Scraped At:</strong> {self.metadata.get('scraped_at', '')}</p>
            </div>
            <hr>
        """
        
        for i, post in enumerate(self.posts, 1):
            html_content += f"""
            <div class="post" id="post-{i}">
                <h3>Post #{i}</h3>
                <div class="post-meta">
                    <p><strong>User:</strong> {post.get('username', 'Unknown')}</p>
                    <p><strong>Timestamp:</strong> {post.get('timestamp', 'Unknown')}</p>
                </div>
                <div class="post-content">
                    <p>{post.get('content', '').replace(chr(10), '<br>')}</p>
                </div>
                <hr>
            </div>
            """
        
        html_content += """
        </body>
        </html>
        """
        
        return html_content

    def convert_to_markdown(self, output_file=None):
        """Convert scraped content to markdown using pandoc"""
        if not output_file:
            thread_title = self.metadata.get('thread_title', 'niniesite_thread')
            # Clean filename - preserve Persian characters
            safe_title = re.sub(r'[<>:"/\\|?*]', '', thread_title)  # Remove only filesystem-unsafe chars
            safe_title = re.sub(r'\s+', '_', safe_title.strip())  # Replace spaces with underscores
            if not safe_title or len(safe_title) < 3:
                safe_title = f"thread_{self.metadata.get('thread_id', 'unknown')}"
            output_file = f"{safe_title}.md"
        
        # Create temporary HTML file
        html_content = self.posts_to_html()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as temp_html:
            temp_html.write(html_content)
            temp_html_path = temp_html.name
        
        try:
            # Convert HTML to Markdown using pandoc
            cmd = ['pandoc', '-f', 'html', '-t', 'markdown', temp_html_path, '-o', output_file]
            subprocess.run(cmd, check=True)
            print(f"Converted to markdown: {output_file}")
            return output_file
        except subprocess.CalledProcessError as e:
            print(f"Error converting to markdown: {e}")
            print("Make sure pandoc is installed: https://pandoc.org/installing.html")
            return None
        except FileNotFoundError:
            print("Pandoc not found. Please install pandoc: https://pandoc.org/installing.html")
            return None
        finally:
            # Clean up temporary file
            if os.path.exists(temp_html_path):
                os.unlink(temp_html_path)


def main():
    parser = argparse.ArgumentParser(description='Scrape Niniesite discussion threads and convert to markdown')
    parser.add_argument('url', help='Niniesite thread URL')
    parser.add_argument('-o', '--output', help='Output markdown file name')
    parser.add_argument('--json', help='Also save raw data as JSON file')
    
    args = parser.parse_args()
    
    scraper = NiniesiteScraper()
    
    try:
        posts, metadata = scraper.scrape_thread(args.url)
        
        if args.json:
            with open(args.json, 'w', encoding='utf-8') as f:
                json.dump({'metadata': metadata, 'posts': posts}, f, ensure_ascii=False, indent=2)
            print(f"Raw data saved to: {args.json}")
        
        markdown_file = scraper.convert_to_markdown(args.output)
        if markdown_file:
            print(f"Successfully created: {markdown_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())