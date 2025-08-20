#!/usr/bin/env python3
"""
get-the-nini - Ninisite Post Scraper
Accepts topic IDs and automatically constructs URLs
"""
import requests
from bs4 import BeautifulSoup
import re
import sys
import time
import argparse
import json
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
import pypandoc


def log_message(message: str):
    """Log message to stderr to avoid interfering with stdout output"""
    print(message, file=sys.stderr)


def construct_default_filename(topic_id: str, fmt: str) -> str:
    """Construct a default filename based on topic ID and format."""
    ext = {"org": ".org", "md": ".md", "json": ".json"}.get(fmt, f".{fmt}")
    return f"ninisite_{topic_id}{ext}"


class OrgWriter:
    """Writer class for streaming org-mode output to file or stdout"""

    def __init__(self, output_file: str = None):
        self.output_file = output_file
        self.file_handle = None
        self._setup_output()

    def _setup_output(self):
        """Setup output destination"""
        if self.output_file == "-":
            self.file_handle = sys.stdout
        elif self.output_file:
            # Check if file exists and warn about truncation
            import os

            if os.path.exists(self.output_file):
                log_message(
                    f"Warning: File '{self.output_file}' exists and will be truncated"
                )
            self.file_handle = open(self.output_file, "w", encoding="utf-8")
        else:
            # Will be set later when we know the topic ID
            self.file_handle = None

    def set_auto_filename(self, topic_id: str):
        """Set auto-generated filename based on topic ID"""
        if not self.file_handle:
            import os

            self.output_file = construct_default_filename(topic_id, "org")
            if os.path.exists(self.output_file):
                log_message(
                    f"Warning: File '{self.output_file}' exists and will be truncated"
                )
            self.file_handle = open(self.output_file, "w", encoding="utf-8")

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
            if self.output_file and self.output_file != "-":
                log_message(f"Successfully saved to {self.output_file}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class NinisiteScraper:
    def __init__(self, sleep_duration: float = 0.0):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )
        self.sleep_duration = sleep_duration

    def maybe_sleep(self):
        """Sleep between requests if sleep_duration > 0"""
        if self.sleep_duration > 0:
            time.sleep(self.sleep_duration)

    def fetch_page(self, url: str) -> BeautifulSoup:
        """Fetch a single page and return BeautifulSoup object"""
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except Exception as e:
            log_message(f"Error fetching {url}: {e}")
            return None

    def detect_total_pages(self, base_url: str) -> int:
        """Detect total number of pages by checking pagination"""
        soup = self.fetch_page(base_url)
        if not soup:
            return 1
        pagination = soup.find("ul", class_="pagination")
        if not pagination:
            return 1
        # Look for page numbers in pagination
        page_links = pagination.find_all("a")
        max_page = 1
        for link in page_links:
            href = link.get("href", "")
            # Extract page number from URL
            if "page=" in href:
                try:
                    page_num = int(href.split("page=")[1].split("&")[0])
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
            pagination = soup.find("ul", class_="pagination")
            if pagination:
                # Look for next page link (< symbol)
                next_links = pagination.find_all("a", title="Next page")
                if next_links:
                    next_href = next_links[0].get("href")
                    if next_href and next_href != "#":
                        current_url = urljoin(base_url, next_href)
                        page_num += 1
                    else:
                        current_url = None
                else:
                    current_url = None
            else:
                current_url = None
            # Be respectful to the server
            self.maybe_sleep()
        if use_progress:
            pbar.close()
        return pages

    def extract_topic_metadata(self, soup: BeautifulSoup) -> Dict:
        """Extract metadata from the main topic"""
        metadata = {}
        # Topic title
        title_elem = soup.find("h1", class_="topic-title")
        if title_elem:
            title_link = title_elem.find("a")
            metadata["title"] = (
                title_link.get_text().strip()
                if title_link
                else title_elem.get_text().strip()
            )
        # Main topic article
        topic_article = soup.find("article", id="topic")
        if topic_article:
            # Author info
            author_elem = topic_article.find("span", itemprop="name")
            if author_elem:
                metadata["author"] = author_elem.get_text().strip()
            # Date
            date_elem = topic_article.find("meta", itemprop="datepublished")
            if date_elem:
                metadata["date"] = date_elem.get("content")
            # View count
            view_elem = topic_article.find("meta", itemprop="userInteractionCount")
            if view_elem:
                metadata["views"] = view_elem.get("content")
        # Breadcrumb for category
        breadcrumb = soup.find("ol", itemtype="http://schema.org/BreadcrumbList")
        if breadcrumb:
            categories = []
            for item in breadcrumb.find_all("li", itemprop="itemListElement"):
                name_elem = item.find("span", itemprop="name")
                if name_elem:
                    categories.append(name_elem.get_text().strip())
            metadata["categories"] = categories[1:]  # Skip the first "تبادل نظر"
        return metadata

    def extract_posts(self, pages: List[BeautifulSoup]) -> List[Dict]:
        """Extract all posts from all pages"""
        all_posts = []
        for page_num, soup in enumerate(pages, 1):
            # Main topic (only on first page)
            if page_num == 1:
                topic_article = soup.find("article", id="topic")
                if topic_article:
                    post = self.extract_post_data(topic_article, is_main_topic=True)
                    if post:
                        post["page"] = page_num
                        all_posts.append(post)
            # Reply posts
            reply_articles = soup.find_all("article", id=re.compile(r"post-\d+"))
            for article in reply_articles:
                # Skip ads and special content
                if "forum-native-ad" in article.get("class", []):
                    continue
                post = self.extract_post_data(article, is_main_topic=False)
                if post:
                    post["page"] = page_num
                    all_posts.append(post)
        return all_posts

    def extract_post_data(self, article, is_main_topic=False) -> Optional[Dict]:
        """Extract data from a single post article"""
        post = {}
        # Post ID
        post_id = article.get("id")
        if post_id:
            post["id"] = post_id
        # Author info
        author_elem = article.find("span", itemprop="name")
        if author_elem:
            post["author"] = author_elem.get_text().strip()
        # Author link for profile
        author_link = article.find("a", itemprop="url")
        if author_link:
            post["author_profile"] = author_link.get("href")
        # Join date and post count
        reg_date_elem = article.find("div", class_="reg-date")
        if reg_date_elem:
            post["author_join_date"] = reg_date_elem.get_text().strip()
        post_count_elem = article.find("div", class_="post-count")
        if post_count_elem:
            post["author_post_count"] = post_count_elem.get_text().strip()
        # Post date/time
        date_elem = article.find("meta", itemprop="datepublished")
        if date_elem:
            post["date"] = date_elem.get("content")
        # Post content
        message_elem = article.find("div", class_="post-message")
        if message_elem:
            # Convert HTML to org-mode using pandoc
            content = self.html_to_org_mode(str(message_elem))
            post["content"] = content
            # Also get HTML for potential formatting
            post["content_html"] = str(message_elem)
        # Quote/reply reference
        quote_elem = article.find("div", class_="topic-post__quotation")
        if quote_elem:
            reply_msg = quote_elem.find("div", class_="reply-message")
            if reply_msg:
                post["quoted_content"] = reply_msg.get_text().strip()
                # Get the referenced post ID
                ref_id = reply_msg.get("data-id")
                if ref_id:
                    post["reply_to_id"] = ref_id
        # Like count
        like_elem = article.find("a", class_="like-count")
        if like_elem:
            like_span = like_elem.find("span")
            if like_span:
                post["likes"] = like_span.get_text().strip()
        # Signature
        signature_elem = article.find("div", class_="topic-post__signature")
        if signature_elem:
            post["signature"] = signature_elem.get_text().strip()
        post["is_main_topic"] = is_main_topic
        return post if post.get("content") or post.get("is_main_topic") else None

    def parse_date_to_jalali(self, date_str: str) -> str:
        """Convert date string to Jalali format in Tehran timezone"""
        try:
            # Parse the date string like "7/4/2023 8:02:48 AM"
            dt = datetime.strptime(date_str, "%m/%d/%Y %I:%M:%S %p")
            # Assume it's already in Tehran timezone (Asia/Tehran)
            tehran_tz = pytz.timezone("Asia/Tehran")
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
            j_year = (
                year - 621 if month < 3 or (month == 3 and day < 21) else year - 620
            )
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
            match = re.search(r"(\d{4}/\d{2}/\d{2})", author_join_date)
            if match:
                join_date = match.group(1)
        if author_post_count:
            # Extract just the number from "تعداد پست: 674"
            match = re.search(r"(\d+)", author_post_count)
            if match:
                post_count = match.group(1)
        return join_date, post_count

    def html_to_org_mode(self, html_content: str) -> str:
        """Convert HTML content to org-mode using pypandoc"""
        try:
            # Convert HTML to org-mode
            org_content = pypandoc.convert_text(html_content, "org", format="html")
            # Replace pandoc's hard line breaks (\ followed by a newline) with a
            # paragraph break (two newlines).
            org_content = org_content.replace("\\\n", "\n\n")
            return org_content.strip()
        except Exception as e:
            log_message(
                f"Warning: pypandoc conversion failed: {e}, falling back to text with breaks"
            )
            return self.html_to_text_with_breaks(html_content)

    def html_to_markdown(self, html_content: str) -> str:
        """Convert HTML content to Markdown using pypandoc, with text fallback"""
        try:
            md_content = pypandoc.convert_text(html_content, "md", format="html")
            return md_content.strip()
        except Exception as e:
            log_message(
                f"Warning: pypandoc conversion (md) failed: {e}, falling back to text with breaks"
            )
            return self.html_to_text_with_breaks(html_content)

    def html_to_text_with_breaks(self, html_content: str) -> str:
        """Convert HTML to text while preserving line breaks"""
        soup = BeautifulSoup(html_content, "html.parser")
        # Replace <p> and <br> tags with newlines
        for br in soup.find_all(["br"]):
            br.replace_with("\n")
        for p in soup.find_all(["p"]):
            p.append("\n")
        # Get text and clean up multiple newlines
        text = soup.get_text()
        # Replace multiple consecutive newlines with double newlines
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def format_author_name(self, author: str) -> str:
        """Add bidi marks around author names that start with Persian characters"""
        if not author:
            return author
        # Check if the first character is Persian/Arabic
        # Persian/Arabic Unicode ranges: 0x0600-0x06FF, 0x0750-0x077F, 0xFB50-0xFDFF, 0xFE70-0xFEFF
        first_char = author[0]
        is_persian = (
            "\u0600" <= first_char <= "\u06ff"  # Arabic
            or "\u0750" <= first_char <= "\u077f"  # Arabic Supplement
            or "\ufb50" <= first_char <= "\ufdff"  # Arabic Presentation Forms-A
            or "\ufe70" <= first_char <= "\ufeff"  # Arabic Presentation Forms-B
        )
        if is_persian:
            # Add Right-to-Left Isolate (RLI) and Pop Directional Isolate (PDI) marks
            return f"\u2067{author}\u2069"  # RLI + author + PDI
        else:
            return author

    def extract_topic_id(self, url: str) -> str:
        """Extract topic ID from URL"""
        parsed_url = urlparse(url)
        path_parts = parsed_url.path.split("/")
        # Look for numeric topic ID in path
        for part in path_parts:
            if part.isdigit():
                return part
        return "unknown"

    def format_markdown(
        self, metadata: Dict, posts: List[Dict], base_url: str, paginate: bool = True
    ) -> str:
        """Format the scraped data as Markdown"""
        lines: List[str] = []
        title = metadata.get("title", "Ninisite Post")
        lines.append(f"# {title}")
        lines.append("")
        # Metadata
        unique_authors = len(set(post.get("author", "Unknown") for post in posts))
        num_pages = max(post.get("page", 1) for post in posts) if posts else 1
        topic_id = self.extract_topic_id(base_url)
        lines.append("**Metadata**")
        lines.append("")
        meta_items = [
            ("Topic ID", topic_id),
            ("Original URL", base_url),
            ("Total Pages", str(num_pages)),
            ("Unique Authors", str(unique_authors)),
            ("Total Posts", str(len(posts))),
        ]
        if metadata.get("author"):
            meta_items.append(("Author", metadata["author"]))
        if metadata.get("date"):
            meta_items.append(("Date", metadata["date"]))
        if metadata.get("views"):
            meta_items.append(("Views", metadata["views"]))
        if metadata.get("categories"):
            meta_items.append(("Categories", " > ".join(metadata["categories"])))
        for k, v in meta_items:
            lines.append(f"- {k}: {v}")
        lines.append("")

        def page_heading(pn: int) -> str:
            if 10 <= pn % 100 <= 20:
                suffix = "th"
            else:
                suffix = {1: "st", 2: "nd", 3: "rd"}.get(pn % 10, "th")
            return f"{pn}{suffix} Page"

        def write_post_md(post: Dict, heading_level: int = 3):
            likes = post.get("likes", "0")
            author = post.get("author", "Unknown")
            join_date, post_count = self.clean_author_info(
                post.get("author_join_date", ""), post.get("author_post_count", "")
            )
            date_formatted = self.parse_date_to_jalali(post.get("date", ""))
            likes_str = (
                f"@likes/{likes} " if likes and likes != "0" and int(likes) > 0 else ""
            )
            author_info = (
                f" ({join_date}, {post_count} posts)"
                if join_date and post_count
                else ""
            )
            hashes = "#" * heading_level
            lines.append(
                f"{hashes} {likes_str}{author}{author_info} [{date_formatted}]"
            )
            lines.append("")
            # Properties (as a definition list-ish)
            props: List[str] = []
            if post.get("id"):
                cid = (
                    post["id"].replace("post-", "")
                    if post["id"].startswith("post-")
                    else post["id"]
                )
                props.append(f"Custom ID: {cid}")
            if post.get("reply_to_id"):
                rid = (
                    post["reply_to_id"].replace("post-", "")
                    if post["reply_to_id"].startswith("post-")
                    else post["reply_to_id"]
                )
                props.append(f"In Reply To: #{rid}")
            if post.get("likes"):
                props.append(f"Likes: {post['likes']}")
            if post.get("page"):
                props.append(f"Page: {post['page']}")
            if props:
                for p in props:
                    lines.append(f"- {p}")
                lines.append("")
            # Quoted content
            if post.get("quoted_content"):
                lines.append("> " + post["quoted_content"].replace("\n", "\n> "))
                lines.append("")
            # Main content (prefer HTML->MD if available)
            content_md = None
            if post.get("content_html"):
                content_md = self.html_to_markdown(post["content_html"]) or ""
            elif post.get("content"):
                # content is org-like; use as-is
                content_md = post["content"]
            if content_md:
                lines.append(content_md)
                lines.append("")
            # Signature
            if post.get("signature"):
                lines.append(f"{hashes}# Signature")
                lines.append(post["signature"])
                lines.append("")

        if paginate:
            posts_by_page: Dict[int, List[Dict]] = {}
            for post in posts:
                pn = post.get("page", 1)
                posts_by_page.setdefault(pn, []).append(post)
            for pn in sorted(posts_by_page.keys()):
                # Build page URL
                if pn == 1:
                    page_url = base_url
                else:
                    if "?" in base_url:
                        page_url = (
                            re.sub(r"page=\d+", f"page={pn}", base_url)
                            if "page=" in base_url
                            else f"{base_url}&page={pn}"
                        )
                    else:
                        page_url = f"{base_url}?page={pn}"
                lines.append(f"## [{page_heading(pn)}]({page_url})")
                lines.append("")
                for post in posts_by_page[pn]:
                    write_post_md(post, heading_level=3)
        else:
            for post in posts:
                write_post_md(post, heading_level=2)
        return "\n".join(lines).rstrip() + "\n"

    def format_json(
        self, metadata: Dict, posts: List[Dict], base_url: str, paginate: bool = True
    ) -> str:
        """Format the scraped data as JSON"""
        unique_authors = len(set(post.get("author", "Unknown") for post in posts))
        num_pages = max(post.get("page", 1) for post in posts) if posts else 1
        topic_id = self.extract_topic_id(base_url)

        def norm_id(v: Optional[str]) -> Optional[str]:
            if not v:
                return v
            return v.replace("post-", "") if v.startswith("post-") else v

        out = {
            "title": metadata.get("title", "Ninisite Post"),
            "topic_id": topic_id,
            "original_url": base_url,
            "total_pages": num_pages,
            "unique_authors": unique_authors,
            "total_posts": len(posts),
            "author": metadata.get("author"),
            "date": metadata.get("date"),
            "views": metadata.get("views"),
            "categories": metadata.get("categories", []),
            "posts": [],
        }
        for post in posts:
            item = {
                "id": norm_id(post.get("id")),
                "author": post.get("author"),
                "author_profile": post.get("author_profile"),
                "author_join_date": post.get("author_join_date"),
                "author_post_count": post.get("author_post_count"),
                "date": post.get("date"),
                "likes": post.get("likes"),
                "page": post.get("page"),
                "reply_to_id": norm_id(post.get("reply_to_id")),
                "quoted_content": post.get("quoted_content"),
                "signature": post.get("signature"),
                # Include multiple representations of content
                "content_org": post.get("content"),
                "content_html": post.get("content_html"),
            }
            # Add a plain text version
            if post.get("content_html"):
                item["content_text"] = self.html_to_text_with_breaks(
                    post["content_html"]
                )
            elif post.get("content"):
                item["content_text"] = post["content"]
            out["posts"].append(item)
        return json.dumps(out, ensure_ascii=False, indent=2)

    def format_org_mode_streaming(
        self,
        metadata: Dict,
        posts: List[Dict],
        base_url: str,
        writer: OrgWriter,
        paginate: bool = True,
    ):
        """Format the scraped data as org-mode with streaming output"""
        # File title
        title = metadata.get("title", "Ninisite Post")
        writer.writeln(f"#+TITLE: {title}")
        writer.writeln()
        # Calculate additional metadata
        unique_authors = len(set(post.get("author", "Unknown") for post in posts))
        num_pages = max(post.get("page", 1) for post in posts) if posts else 1
        scrape_time = self.parse_date_to_jalali(
            datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        )
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
        if metadata.get("author"):
            writer.writeln(f":AUTHOR: {metadata['author']}")
        if metadata.get("date"):
            writer.writeln(f":DATE: {metadata['date']}")
        if metadata.get("views"):
            writer.writeln(f":VIEWS: {metadata['views']}")
        if metadata.get("categories"):
            writer.writeln(f":CATEGORIES: {' > '.join(metadata['categories'])}")
        writer.writeln(f":TOTAL_POSTS: {len(posts)}")
        writer.writeln(":END:")
        writer.writeln()
        if paginate:
            # Group posts by page
            posts_by_page = {}
            for post in posts:
                page_num = post.get("page", 1)
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
                    if "?" in base_url:
                        if "page=" in base_url:
                            # Replace existing page parameter
                            page_url = re.sub(r"page=\d+", f"page={page_num}", base_url)
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

    def format_org_mode(
        self, metadata: Dict, posts: List[Dict], base_url: str, paginate: bool = True
    ) -> str:
        """Format the scraped data as org-mode (non-streaming version)"""
        org_content = []
        # File title
        title = metadata.get("title", "Ninisite Post")
        org_content.append(f"#+TITLE: {title}")
        org_content.append("")
        # Calculate additional metadata
        unique_authors = len(set(post.get("author", "Unknown") for post in posts))
        num_pages = max(post.get("page", 1) for post in posts) if posts else 1
        scrape_time = self.parse_date_to_jalali(
            datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        )
        topic_id = self.extract_topic_id(base_url)
        # Main header
        org_content.append(f"* {title}")
        org_content.append(":PROPERTIES:")
        if metadata.get("author"):
            org_content.append(f":AUTHOR: {metadata['author']}")
        if metadata.get("categories"):
            org_content.append(f":CATEGORIES: {' > '.join(metadata['categories'])}")
        org_content.append(f":TOTAL_POSTS: {len(posts)}")
        if metadata.get("views"):
            org_content.append(f":VIEWS: {metadata['views']}")
        org_content.append(f":TOPIC_ID: {topic_id}")
        org_content.append(f":ORIGINAL_URL: {base_url}")
        org_content.append(f":TOTAL_PAGES: {num_pages}")
        org_content.append(f":UNIQUE_AUTHORS: {unique_authors}")
        if metadata.get("date"):
            org_content.append(f":DATE: {metadata['date']}")
        org_content.append(f":SCRAPE_DATE: {scrape_time}")
        org_content.append(":END:")
        org_content.append("")
        if paginate:
            # Group posts by page
            posts_by_page = {}
            for post in posts:
                page_num = post.get("page", 1)
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
                    if "?" in base_url:
                        if "page=" in base_url:
                            # Replace existing page parameter
                            page_url = re.sub(r"page=\d+", f"page={page_num}", base_url)
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
        return "\n".join(org_content)

    def _format_post_streaming(
        self, post: Dict, writer: OrgWriter, heading_level: str = "**"
    ):
        """Format a single post and write to streaming output"""
        # Format heading with @likes/{count} author (join_date, post_count posts) [date]
        likes = post.get("likes", "0")
        author = post.get("author", "Unknown")
        author_formatted = self.format_author_name(author)
        date_formatted = self.parse_date_to_jalali(post.get("date", ""))
        join_date, post_count = self.clean_author_info(
            post.get("author_join_date", ""), post.get("author_post_count", "")
        )
        author_info = (
            f"({join_date}, {post_count} posts)" if join_date and post_count else ""
        )
        # Only include likes if > 0, and use @likes/{count} format
        likes_str = ""
        if likes and likes != "0" and int(likes) > 0:
            likes_str = f"@likes/{likes} "
        heading = f"{heading_level} {likes_str}{author_formatted} {author_info} [{date_formatted}]"
        writer.writeln(heading)
        # Post properties
        writer.writeln(":PROPERTIES:")
        if post.get("id"):
            # Extract post ID for CUSTOM_ID (remove 'post-' prefix if present)
            custom_id = (
                post["id"].replace("post-", "")
                if post["id"].startswith("post-")
                else post["id"]
            )
            writer.writeln(f":CUSTOM_ID: {custom_id}")
        if post.get("author"):
            writer.writeln(f":AUTHOR: {post['author']}")
        if post.get("date"):
            writer.writeln(f":DATE: {post['date']}")
        if post.get("author_join_date"):
            writer.writeln(f":AUTHOR_JOIN_DATE: {post['author_join_date']}")
        if post.get("author_post_count"):
            writer.writeln(f":AUTHOR_POST_COUNT: {post['author_post_count']}")
        if post.get("likes"):
            writer.writeln(f":LIKES: {post['likes']}")
        if post.get("page"):
            writer.writeln(f":PAGE: {post['page']}")
        if post.get("reply_to_id"):
            writer.writeln(f":REPLY_TO_ID: {post['reply_to_id']}")
        writer.writeln(":END:")
        # Reply link (if replying to someone)
        if post.get("reply_to_id"):
            reply_id = (
                post["reply_to_id"].replace("post-", "")
                if post["reply_to_id"].startswith("post-")
                else post["reply_to_id"]
            )
            writer.writeln(f"- [[#{reply_id}][In Reply To]]")
            writer.writeln()
        # Quoted content (if replying to someone)
        if post.get("quoted_content"):
            writer.writeln("#+begin_quote")
            quoted_lines = post["quoted_content"].split("\n")
            for line in quoted_lines:
                if line.strip():
                    writer.writeln(line.strip())
            writer.writeln("#+end_quote")
            writer.writeln()
        # Main content
        if post.get("content"):
            content_lines = post["content"].split("\n")
            for line in content_lines:
                if line.strip():
                    writer.writeln(line.strip())
            writer.writeln()
        # Signature
        if post.get("signature"):
            signature_level = "***" if heading_level == "**" else "****"
            writer.writeln(f"{signature_level} Signature:")
            sig_lines = post["signature"].split("\n")
            for line in sig_lines:
                if line.strip():
                    writer.writeln(line.strip())
            writer.writeln()

    def _format_post(
        self, post: Dict, org_content: List[str], heading_level: str = "**"
    ):
        """Format a single post and append to org_content"""
        # Format heading with @likes/{count} author (join_date, post_count posts) [date]
        likes = post.get("likes", "0")
        author = post.get("author", "Unknown")
        author_formatted = self.format_author_name(author)
        date_formatted = self.parse_date_to_jalali(post.get("date", ""))
        join_date, post_count = self.clean_author_info(
            post.get("author_join_date", ""), post.get("author_post_count", "")
        )
        author_info = (
            f"({join_date}, {post_count} posts)" if join_date and post_count else ""
        )
        # Only include likes if > 0, and use @likes/{count} format
        likes_str = ""
        if likes and likes != "0" and int(likes) > 0:
            likes_str = f"@likes/{likes} "
        heading = f"{heading_level} {likes_str}{author_formatted} {author_info} [{date_formatted}]"
        org_content.append(heading)
        # Post properties
        org_content.append(":PROPERTIES:")
        if post.get("id"):
            # Extract post ID for CUSTOM_ID (remove 'post-' prefix if present)
            custom_id = (
                post["id"].replace("post-", "")
                if post["id"].startswith("post-")
                else post["id"]
            )
            org_content.append(f":CUSTOM_ID: {custom_id}")
        if post.get("author"):
            org_content.append(f":AUTHOR: {post['author']}")
        if post.get("date"):
            org_content.append(f":DATE: {post['date']}")
        if post.get("author_join_date"):
            org_content.append(f":AUTHOR_JOIN_DATE: {post['author_join_date']}")
        if post.get("author_post_count"):
            org_content.append(f":AUTHOR_POST_COUNT: {post['author_post_count']}")
        if post.get("likes"):
            org_content.append(f":LIKES: {post['likes']}")
        if post.get("page"):
            org_content.append(f":PAGE: {post['page']}")
        if post.get("reply_to_id"):
            org_content.append(f":REPLY_TO_ID: {post['reply_to_id']}")
        org_content.append(":END:")
        # Reply link (if replying to someone)
        if post.get("reply_to_id"):
            reply_id = (
                post["reply_to_id"].replace("post-", "")
                if post["reply_to_id"].startswith("post-")
                else post["reply_to_id"]
            )
            org_content.append(f"- [[#{reply_id}][In Reply To]]")
            org_content.append("")
        # Quoted content (if replying to someone)
        if post.get("quoted_content"):
            org_content.append("#+begin_quote")
            quoted_lines = post["quoted_content"].split("\n")
            for line in quoted_lines:
                if line.strip():
                    org_content.append(line.strip())
            org_content.append("#+end_quote")
            org_content.append("")
        # Main content
        if post.get("content"):
            content_lines = post["content"].split("\n")
            for line in content_lines:
                if line.strip():
                    org_content.append(line.strip())
            org_content.append("")
        # Signature
        if post.get("signature"):
            signature_level = "***" if heading_level == "**" else "****"
            org_content.append(f"{signature_level} Signature:")
            sig_lines = post["signature"].split("\n")
            for line in sig_lines:
                if line.strip():
                    org_content.append(line.strip())
            org_content.append("")

    def scrape_discussion_streaming(
        self, url: str, writer: OrgWriter, paginate: bool = True
    ):
        """Main method to scrape a discussion and stream org-mode formatted content"""
        log_message(f"Starting to scrape: {url}")
        # Detect total pages for progress tracking
        total_pages = self.detect_total_pages(url)
        # Set up progress bar if tqdm is available and stdout is a tty
        use_progress = HAS_TQDM and sys.stdout.isatty()
        if use_progress:
            pbar = tqdm(total=total_pages, desc="Fetching pages", unit="page")
        pages = []
        current_url = url
        page_num = 1
        all_posts = []
        metadata = None
        # Write header info as soon as we have it
        first_page_processed = False
        while current_url:
            if not use_progress:
                log_message(f"Fetching page {page_num}/{total_pages}: {current_url}")
            soup = self.fetch_page(current_url)
            if not soup:
                break
            # Extract metadata from first page and write header
            if not first_page_processed:
                metadata = self.extract_topic_metadata(soup)
                self.write_header_streaming(metadata, url, writer, total_pages)
                first_page_processed = True
            # Extract and process posts from this page
            page_posts = self.extract_posts_from_page(soup, page_num)
            all_posts.extend(page_posts)
            # Write page content immediately
            if paginate and page_posts:
                self.write_page_streaming(page_num, url, page_posts, writer)
            elif not paginate:
                # Write posts directly under main heading
                for post in page_posts:
                    self._format_post_streaming(post, writer, heading_level="**")
            if use_progress:
                pbar.update(1)
            # Find next page URL
            pagination = soup.find("ul", class_="pagination")
            if pagination:
                next_links = pagination.find_all("a", title="Next page")
                if next_links:
                    next_href = next_links[0].get("href")
                    if next_href and next_href != "#":
                        current_url = urljoin(url, next_href)
                        page_num += 1
                    else:
                        current_url = None
                else:
                    current_url = None
            else:
                current_url = None
            # Be respectful to the server
            self.maybe_sleep()
        if use_progress:
            pbar.close()
        if not all_posts:
            raise Exception("No posts found")
        log_message(f"Extracted {len(all_posts)} posts from {page_num} pages")
        # Write final summary at the end
        self.write_summary_streaming(all_posts, writer)

    def extract_posts_from_page(self, soup: BeautifulSoup, page_num: int) -> List[Dict]:
        """Extract posts from a single page"""
        posts = []
        # Main topic (only on first page)
        if page_num == 1:
            topic_article = soup.find("article", id="topic")
            if topic_article:
                post = self.extract_post_data(topic_article, is_main_topic=True)
                if post:
                    post["page"] = page_num
                    posts.append(post)
        # Reply posts
        reply_articles = soup.find_all("article", id=re.compile(r"post-\d+"))
        for article in reply_articles:
            # Skip ads and special content
            if "forum-native-ad" in article.get("class", []):
                continue
            post = self.extract_post_data(article, is_main_topic=False)
            if post:
                post["page"] = page_num
                posts.append(post)
        return posts

    def write_header_streaming(
        self, metadata: Dict, base_url: str, writer: OrgWriter, total_pages: int
    ):
        """Write the file header and main topic metadata"""
        # File title
        title = metadata.get("title", "Ninisite Post")
        writer.writeln(f"#+TITLE: {title}")
        writer.writeln()
        # Calculate metadata (we'll update unique authors count later, but we need placeholders)
        scrape_time = self.parse_date_to_jalali(
            datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        )
        topic_id = self.extract_topic_id(base_url)
        # Set auto filename if needed
        writer.set_auto_filename(topic_id)
        # Main header
        writer.writeln(f"* {title}")
        writer.writeln(":PROPERTIES:")
        writer.writeln(f":TOPIC_ID: {topic_id}")
        writer.writeln(f":ORIGINAL_URL: {base_url}")
        writer.writeln(f":SCRAPE_DATE: {scrape_time}")
        writer.writeln(f":TOTAL_PAGES: {total_pages}")
        if metadata.get("author"):
            writer.writeln(f":AUTHOR: {metadata['author']}")
        if metadata.get("date"):
            writer.writeln(f":DATE: {metadata['date']}")
        if metadata.get("views"):
            writer.writeln(f":VIEWS: {metadata['views']}")
        if metadata.get("categories"):
            writer.writeln(f":CATEGORIES: {' > '.join(metadata['categories'])}")
        # Note: TOTAL_POSTS and UNIQUE_AUTHORS will be added at the end for streaming mode
        writer.writeln(":END:")
        writer.writeln()

    def write_page_streaming(
        self, page_num: int, base_url: str, posts: List[Dict], writer: OrgWriter
    ):
        """Write a page section with its posts"""
        # Create page URL
        if page_num == 1:
            page_url = base_url
        else:
            # Add or modify page parameter
            if "?" in base_url:
                if "page=" in base_url:
                    # Replace existing page parameter
                    page_url = re.sub(r"page=\d+", f"page={page_num}", base_url)
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
        for post in posts:
            self._format_post_streaming(post, writer, heading_level="***")

    def write_summary_streaming(self, all_posts: List[Dict], writer: OrgWriter):
        """Write final summary statistics"""
        unique_authors = len(set(post.get("author", "Unknown") for post in all_posts))
        writer.writeln()
        writer.writeln("* Summary")
        writer.writeln(f"- Total posts: {len(all_posts)}")
        writer.writeln(f"- Unique authors: {unique_authors}")

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


def construct_url_from_topic_id(topic_id: str) -> str:
    """Construct ninisite URL from topic ID"""
    return f"https://www.ninisite.com/discussion/topic/{topic_id}/"


def is_valid_topic_id(topic_id: str) -> bool:
    """Check if the provided string is a valid topic ID (numeric)"""
    return topic_id.isdigit()


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Scrape Ninisite discussion posts and output in org, markdown, or JSON. "
            "Accepts topic IDs or full URLs."
        )
    )
    parser.add_argument(
        "topic_id_or_url",
        nargs="*",
        help="Ninisite topic ID (e.g. '11473285') or full discussion URL to scrape",
    )
    parser.add_argument(
        "--paginate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Organize posts under page headings (default: True)",
    )
    parser.add_argument(
        "--streaming",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Stream output as posts are processed (default: True)",
    )
    parser.add_argument(
        "-o",
        "--out",
        help='Output file (use "-" for stdout, default: auto-generate from topic ID)',
    )
    parser.add_argument(
        "--format",
        "--fmt",
        dest="format",
        choices=["auto", "org", "md", "markdown", "json"],
        default="auto",
        help=(
            "Output format: org, md/markdown, or json. Default: auto (guesses from output "
            "file extension; when no output path provided, defaults to org)."
        ),
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.0,
        help="Sleep duration between requests in seconds (default: 0)",
    )
    args = parser.parse_args()
    # Handle URL/topic ID parsing
    if not args.topic_id_or_url:
        parser.error("At least one topic ID or URL is required")
    if len(args.topic_id_or_url) > 1:
        parser.error("Only one topic ID or URL is supported at this time")
    input_value = args.topic_id_or_url[0]
    # Determine if input is a topic ID or full URL
    if is_valid_topic_id(input_value):
        # It's a topic ID, construct the URL
        url = construct_url_from_topic_id(input_value)
        log_message(f"Topic ID provided: {input_value}")
        log_message(f"Constructed URL: {url}")
    elif input_value.startswith("http"):
        # It's already a URL
        url = input_value
        log_message(f"Full URL provided: {url}")
    else:
        parser.error(
            f"Invalid input: '{input_value}' is neither a valid topic ID nor a URL"
        )
    scraper = NinisiteScraper(sleep_duration=args.sleep)

    def resolve_format(fmt_opt: str, out_path: Optional[str]) -> str:
        fmt = fmt_opt or "auto"
        fmt = "md" if fmt == "markdown" else fmt
        if fmt == "auto":
            if out_path and out_path != "-":
                lower = out_path.lower()
                if lower.endswith(".org"):
                    return "org"
                if lower.endswith(".md") or lower.endswith(".markdown"):
                    return "md"
                if lower.endswith(".json"):
                    return "json"
            # No path provided or stdout: default to org
            return "org"
        return fmt

    fmt = resolve_format(args.format, args.out)
    try:
        # Log output mode information
        if args.out == "-":
            log_message(
                f"Output mode: {'streaming' if args.streaming else 'buffered'} to stdout (format={fmt})"
            )
        elif args.out:
            log_message(
                f"Output mode: {'streaming' if args.streaming else 'buffered'} to file '{args.out}' (format={fmt})"
            )
        else:
            topic_id = scraper.extract_topic_id(url)
            output_file = construct_default_filename(topic_id, fmt)
            log_message(
                f"Output mode: {'streaming' if args.streaming else 'buffered'} to auto-generated file '{output_file}' (format={fmt})"
            )
        # Non-org formats do not support streaming; fall back to buffered
        if fmt != "org" and args.streaming:
            log_message(
                "Note: streaming is only supported for org format; falling back to buffered output"
            )
            args.streaming = False
        if args.streaming and fmt == "org":
            # Streaming org-mode
            with OrgWriter(args.out) as writer:
                scraper.scrape_discussion_streaming(url, writer, args.paginate)
        else:
            # Buffered modes
            # Fetch data once
            pages = scraper.get_all_pages(url)
            if not pages:
                raise Exception("Could not fetch any pages")
            metadata = scraper.extract_topic_metadata(pages[0])
            posts = scraper.extract_posts(pages)
            if fmt == "org":
                content = scraper.format_org_mode(metadata, posts, url, args.paginate)
            elif fmt == "md":
                content = scraper.format_markdown(metadata, posts, url, args.paginate)
            elif fmt == "json":
                content = scraper.format_json(metadata, posts, url, args.paginate)
            else:
                raise Exception(f"Unsupported format: {fmt}")
            # Handle output
            if args.out == "-":
                print(content)
            elif args.out:
                import os

                if os.path.exists(args.out):
                    log_message(
                        f"Warning: File '{args.out}' exists and will be truncated"
                    )
                with open(args.out, "w", encoding="utf-8") as f:
                    f.write(content)
                log_message(f"Successfully saved to {args.out}")
            else:
                import os

                topic_id = scraper.extract_topic_id(url)
                output_file = construct_default_filename(topic_id, fmt)
                if os.path.exists(output_file):
                    log_message(
                        f"Warning: File '{output_file}' exists and will be truncated"
                    )
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(content)
                log_message(f"Successfully saved to {output_file}")
    except Exception as e:
        log_message(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
