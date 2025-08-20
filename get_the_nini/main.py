#!/usr/bin/env python3
"""
get-the-nini - Ninisite Post Scraper
Accepts topic IDs and automatically constructs URLs
"""
from pynight.common_icecream import ic
import traceback
import requests
from bs4 import BeautifulSoup, PageElement
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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
    def __init__(
        self,
        sleep_duration: float = 0.0,
        retries: int = 5,
        backoff_factor: float = 3.0,
    ):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )
        self.sleep_duration = sleep_duration
        self.max_retries = retries
        self.backoff_factor = backoff_factor
        # For synchronizing parallel requests
        self.last_request_time = 0
        self.request_lock = threading.Lock()
        self.backoff_event = threading.Event()
        self.backoff_event.set()

    def fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch a single page, with retry and backoff logic."""
        for attempt in range(self.max_retries):
            # 1. Wait if another thread has triggered a global backoff
            self.backoff_event.wait()
            # 2. Ensure sleep duration between consecutive requests across all threads
            with self.request_lock:
                now = time.time()
                elapsed = now - self.last_request_time
                if elapsed < self.sleep_duration:
                    time.sleep(self.sleep_duration - elapsed)
                # Update last request time *after* sleeping, just before the request
                self.last_request_time = time.time()
            # 3. Try to fetch the page
            try:
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                # Success!
                return BeautifulSoup(response.content, "html.parser")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < self.max_retries - 1:
                    sleep_time = self.backoff_factor
                    log_message(
                        f"Warning: Received 429 (Too Many Requests) for {url}. "
                        f"Pausing all threads for {sleep_time}s. Retrying... (Attempt {attempt + 1}/{self.max_retries})"
                    )
                    # Set the global event to make other threads wait
                    self.backoff_event.clear()
                    time.sleep(sleep_time)
                    # Clear the event to allow all threads to proceed
                    self.backoff_event.set()
                    continue  # Go to the next retry attempt
                else:
                    # Other HTTP error or max retries exceeded for 429
                    log_message(f"Error fetching {url}: {e}")
                    return None
            except Exception as e:
                # Other exceptions (timeout, connection error, etc.)
                if attempt < self.max_retries - 1:
                    log_message(
                        f"Warning: Error fetching {url}: {e}. Retrying... (Attempt {attempt + 1}/{self.max_retries})"
                    )
                    time.sleep(1)  # Simple sleep for transient errors
                else:
                    log_message(
                        f"Error fetching {url} after {self.max_retries} attempts: {e}"
                    )
                    return None
        # If all retries fail
        log_message(f"Failed to fetch {url} after {self.max_retries} attempts.")
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

    def _construct_page_url(self, base_url: str, page_num: int) -> str:
        """Construct the URL for a specific page number."""
        if page_num == 1:
            return base_url
        if "?" in base_url:
            if "page=" in base_url:
                # Replace existing page parameter
                return re.sub(r"page=\d+", f"page={page_num}", base_url)
            else:
                # Add page parameter to existing query string
                return f"{base_url}&page={page_num}"
        else:
            # Add page parameter as first query parameter
            return f"{base_url}?page={page_num}"

    def fetch_and_extract_posts(
        self,
        base_url: str,
        parallel: int = 1,
        first_page_soup: Optional[BeautifulSoup] = None,
    ) -> List[Dict]:
        """
        Fetches each page of a discussion and immediately extracts posts from it.
        This combines fetching and processing into a single, memory-efficient process.
        An optional pre-fetched first page can be provided to avoid re-fetching.
        """
        total_pages = self.detect_total_pages(base_url)
        # Dictionary to hold the results and keep them in order
        all_posts_by_page: Dict[int, List[Dict]] = {}
        start_page = 1
        if first_page_soup:
            log_message("Processing provided first page...")
            posts = self.extract_posts_from_page(first_page_soup, 1)
            if posts:
                all_posts_by_page[1] = posts
            start_page = 2  # Start fetching from the second page
        if total_pages < start_page:
            # Only one page, and it was already processed, or no pages to fetch
            return all_posts_by_page.get(1, []) if 1 in all_posts_by_page else []
        log_message(
            f"Fetching and processing pages {start_page}-{total_pages} with up to {parallel} workers..."
        )

        def _fetch_and_extract_worker(page_num: int) -> Optional[List[Dict]]:
            """Worker function to fetch one page and extract its posts."""
            url = self._construct_page_url(base_url, page_num)
            soup = self.fetch_page(url)
            if soup:
                return self.extract_posts_from_page(soup, page_num)
            log_message(f"Worker failed to fetch or process page {page_num}")
            return None

        # If parallel <= 1, run sequentially to avoid thread overhead
        if parallel <= 1:
            # Set up progress bar if tqdm is available and stdout is a tty
            use_progress = HAS_TQDM and sys.stdout.isatty()
            page_range = range(start_page, total_pages + 1)
            if use_progress:
                pbar = tqdm(page_range, desc="Fetching & Processing", unit="page")
            else:
                pbar = page_range
            for page_num in pbar:
                if not use_progress:
                    log_message(
                        f"Fetching and processing page {page_num}/{total_pages}"
                    )
                page_posts = _fetch_and_extract_worker(page_num)
                if page_posts:
                    all_posts_by_page[page_num] = page_posts
            if use_progress:
                pbar.close()
        else:
            use_progress = HAS_TQDM and sys.stdout.isatty()
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                future_to_page_num = {
                    executor.submit(_fetch_and_extract_worker, page_num): page_num
                    for page_num in range(start_page, total_pages + 1)
                }
                completed_futures = as_completed(future_to_page_num)
                if use_progress:
                    completed_futures = tqdm(
                        completed_futures,
                        total=len(future_to_page_num),
                        desc="Fetching & Processing",
                        unit="page",
                    )
                for future in completed_futures:
                    page_num = future_to_page_num[future]
                    try:
                        page_posts = future.result()
                        if page_posts:
                            all_posts_by_page[page_num] = page_posts
                    except Exception as exc:
                        log_message(
                            f"Page {page_num} fetch/process generated an exception: {exc}"
                        )
        if not all_posts_by_page:
            return []
        # Combine the results from all pages in the correct order
        all_posts = []
        for i in sorted(all_posts_by_page.keys()):
            all_posts.extend(all_posts_by_page[i])
        return all_posts

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
            content = self.html_to_org_mode(message_elem, strip_links=False)
            post["content"] = content
            # Also get HTML for potential formatting
            post["content_html"] = str(message_elem)
        # Quote/reply reference
        quote_elem = article.find("div", class_="topic-post__quotation")
        if quote_elem:
            reply_msg = quote_elem.find("div", class_="reply-message")
            if reply_msg:
                post["quoted_content"] = self.html_to_org_mode(reply_msg, strip_links=True)
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
            post["signature"] = self.html_to_org_mode(signature_elem, strip_links=False)
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

    def html_to_org_mode(self, html_content, strip_links: bool = False) -> str:
        """Convert HTML content to org-mode using pypandoc, optionally stripping links."""
        if not isinstance(html_content, PageElement):
            log_message(
                f"Warning: html_content is not a BeautifulSoup PageElement object (type was {type(html_content)}). Please pass a valid object."
            )
            # Depending on requirements, you might want to parse it here or return an error.
            raise ValueError("html_content must be a BeautifulSoup PageElement object.")
            # html_content = BeautifulSoup(str(html_content), 'html.parser')

        # Find all 'i' elements and remove them if they are empty
        for i_tag in html_content.find_all("i"):
            # An element is considered empty if it has no text content after stripping whitespace.
            if not i_tag.get_text(strip=True):
                i_tag.decompose()

        # Strip links if requested
        if strip_links:
            for a_tag in html_content.find_all("a"):
                a_tag.unwrap()  # Replaces the tag with its contents (the text)

        # Convert the modified BeautifulSoup object to a string.
        html_content = str(html_content)

        try:
            # Convert HTML to org-mode
            org_content = pypandoc.convert_text(html_content, "org", format="html")
            # Replace pandoc's hard line breaks (\ followed by a newline) with a
            # paragraph break (two newlines).
            org_content = org_content.replace("\\\\\n", "\n\n")

            # Remove any extra newlines that might have been added
            while "\n\n\n" in org_content:
                org_content = org_content.replace("\n\n\n", "\n\n")

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
        first_char = author
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
                page_url = self._construct_page_url(base_url, pn)
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

    def _generate_org_header_lines(
        self,
        metadata: Dict,
        base_url: str,
        total_pages: int,
        total_posts: Optional[int] = None,
        unique_authors: Optional[int] = None,
    ) -> List[str]:
        """Generate the lines for the org-mode file header and properties."""
        lines = []
        title = metadata.get("title", "Ninisite Post")
        lines.append(f"#+TITLE: {title}")
        lines.append("")
        scrape_time = self.parse_date_to_jalali(
            datetime.now().strftime("%m/%d/%Y %I:%M:%S %p")
        )
        topic_id = self.extract_topic_id(base_url)
        # Main header
        lines.append(f"* {title}")
        lines.append(":PROPERTIES:")
        lines.append(f":TOPIC_ID: {topic_id}")
        lines.append(f":ORIGINAL_URL: {base_url}")
        lines.append(f":SCRAPE_DATE: {scrape_time}")
        lines.append(f":TOTAL_PAGES: {total_pages}")
        if unique_authors is not None:
            lines.append(f":UNIQUE_AUTHORS: {unique_authors}")
        if metadata.get("author"):
            lines.append(f":AUTHOR: {metadata['author']}")
        if metadata.get("date"):
            lines.append(f":DATE: {metadata['date']}")
        if metadata.get("views"):
            lines.append(f":VIEWS: {metadata['views']}")
        if metadata.get("categories"):
            lines.append(f":CATEGORIES: {' > '.join(metadata['categories'])}")
        if total_posts is not None:
            lines.append(f":TOTAL_POSTS: {total_posts}")
        lines.append(":END:")
        lines.append("")
        return lines

    def _generate_org_page_heading_lines(
        self, page_num: int, base_url: str
    ) -> List[str]:
        """Generate the org-mode heading for a page."""
        page_url = self._construct_page_url(base_url, page_num)
        if page_num == 1:
            heading = f"** [[{page_url}][1st Page]]"
        elif page_num == 2:
            heading = f"** [[{page_url}][2nd Page]]"
        elif page_num == 3:
            heading = f"** [[{page_url}][3rd Page]]"
        else:
            heading = f"** [[{page_url}][{page_num}th Page]]"
        return [heading]

    def _generate_post_org_lines(
        self, post: Dict, heading_level: str = "**"
    ) -> List[str]:
        """Generate the org-mode lines for a single post."""
        lines = []
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
        lines.append(heading)
        # Post properties
        lines.append(":PROPERTIES:")
        if post.get("id"):
            # Extract post ID for CUSTOM_ID (remove 'post-' prefix if present)
            custom_id = (
                post["id"].replace("post-", "")
                if post["id"].startswith("post-")
                else post["id"]
            )
            lines.append(f":CUSTOM_ID: {custom_id}")
        if post.get("author"):
            lines.append(f":AUTHOR: {post['author']}")
        if post.get("date"):
            lines.append(f":DATE: {post['date']}")
        if post.get("author_join_date"):
            lines.append(f":AUTHOR_JOIN_DATE: {post['author_join_date']}")
        if post.get("author_post_count"):
            lines.append(f":AUTHOR_POST_COUNT: {post['author_post_count']}")
        if post.get("likes"):
            lines.append(f":LIKES: {post['likes']}")
        if post.get("page"):
            lines.append(f":PAGE: {post['page']}")
        if post.get("reply_to_id"):
            lines.append(f":REPLY_TO_ID: {post['reply_to_id']}")
        lines.append(":END:")
        # Reply link (if replying to someone)
        if post.get("reply_to_id"):
            reply_id = (
                post["reply_to_id"].replace("post-", "")
                if post["reply_to_id"].startswith("post-")
                else post["reply_to_id"]
            )
            lines.append(f"- [[#{reply_id}][In Reply To]]")
            lines.append("")
        # Quoted content (if replying to someone)
        if post.get("quoted_content"):
            lines.append("#+begin_quote")
            lines.append(post["quoted_content"].strip())
            lines.append("#+end_quote")
            lines.append("")
        # Main content
        if post.get("content"):
            lines.append(post["content"].strip())
            lines.append("")
        # Signature
        if post.get("signature"):
            signature_level = "***" if heading_level == "**" else "****"
            lines.append(f"{signature_level} Signature:")
            lines.append(post["signature"].strip())
            lines.append("")
        return lines

    def format_org_mode_streaming(
        self,
        metadata: Dict,
        posts: List[Dict],
        base_url: str,
        writer: OrgWriter,
        paginate: bool = True,
    ):
        """Format the scraped data as org-mode with streaming output"""
        # File title and header
        num_pages = max(post.get("page", 1) for post in posts) if posts else 1
        topic_id = self.extract_topic_id(base_url)
        writer.set_auto_filename(topic_id)
        header_lines = self._generate_org_header_lines(
            metadata,
            base_url,
            num_pages,
            len(posts),
            len(set(p.get("author") for p in posts)),
        )
        for line in header_lines:
            writer.writeln(line)
        if paginate:
            # Group posts by page
            posts_by_page: Dict[int, List[Dict]] = {}
            for post in posts:
                page_num = post.get("page", 1)
                posts_by_page.setdefault(page_num, []).append(post)
            # Process each page
            for page_num in sorted(posts_by_page.keys()):
                for line in self._generate_org_page_heading_lines(page_num, base_url):
                    writer.writeln(line)
                for post in posts_by_page[page_num]:
                    for line in self._generate_post_org_lines(
                        post, heading_level="***"
                    ):
                        writer.writeln(line)
        else:
            # Non-paginated: process all posts directly under main heading
            for post in posts:
                for line in self._generate_post_org_lines(post, heading_level="**"):
                    writer.writeln(line)

    def format_org_mode(
        self, metadata: Dict, posts: List[Dict], base_url: str, paginate: bool = True
    ) -> str:
        """Format the scraped data as org-mode (non-streaming version)"""
        unique_authors = len(set(post.get("author", "Unknown") for post in posts))
        num_pages = max(post.get("page", 1) for post in posts) if posts else 1
        org_content = self._generate_org_header_lines(
            metadata, base_url, num_pages, len(posts), unique_authors
        )
        if paginate:
            # Group posts by page
            posts_by_page: Dict[int, List[Dict]] = {}
            for post in posts:
                page_num = post.get("page", 1)
                posts_by_page.setdefault(page_num, []).append(post)
            # Process each page
            for page_num in sorted(posts_by_page.keys()):
                org_content.extend(
                    self._generate_org_page_heading_lines(page_num, base_url)
                )
                for post in posts_by_page[page_num]:
                    org_content.extend(
                        self._generate_post_org_lines(post, heading_level="***")
                    )
        else:
            # Non-paginated: process all posts directly under main heading
            for post in posts:
                org_content.extend(
                    self._generate_post_org_lines(post, heading_level="**")
                )
        return "\n".join(org_content)

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
        all_posts = []
        metadata = None
        # Write header info as soon as we have it
        first_page_processed = False
        current_url = url
        page_num = 1
        while current_url and page_num <= total_pages:
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
                    for line in self._generate_post_org_lines(post, heading_level="**"):
                        writer.writeln(line)
            if use_progress:
                pbar.update(1)
            page_num += 1
            current_url = self._construct_page_url(url, page_num)
        if use_progress:
            pbar.close()
        if not all_posts:
            raise Exception("No posts found")
        log_message(f"Extracted {len(all_posts)} posts from {page_num - 1} pages")
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
        topic_id = self.extract_topic_id(base_url)
        writer.set_auto_filename(topic_id)
        # Header is generated with placeholders for summary stats
        header_lines = self._generate_org_header_lines(metadata, base_url, total_pages)
        for line in header_lines:
            writer.writeln(line)

    def write_page_streaming(
        self, page_num: int, base_url: str, posts: List[Dict], writer: OrgWriter
    ):
        """Write a page section with its posts"""
        # Page heading
        for line in self._generate_org_page_heading_lines(page_num, base_url):
            writer.writeln(line)
        # Process posts for this page
        for post in posts:
            for line in self._generate_post_org_lines(post, heading_level="***"):
                writer.writeln(line)

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
        # Fetch first page to get metadata
        first_page_soup = self.fetch_page(url)
        if not first_page_soup:
            raise Exception("Could not fetch the first page.")
        metadata = self.extract_topic_metadata(first_page_soup)
        # Fetch and extract all posts. Since this is a simple, non-parallel
        # version, we use parallel=1 and pass the first page.
        posts = self.fetch_and_extract_posts(
            url, parallel=1, first_page_soup=first_page_soup
        )
        if not posts:
            raise Exception("No posts found")
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
        default=False,
        help="Stream output as posts are processed (default: False, org-mode only)",
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
        default=0.2,
        help="Sleep duration between requests in seconds (default: 0.2)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=16,
        help="Number of parallel workers for fetching and processing pages (non-streaming mode only, default: 16)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Number of retries for failed requests (default: 5)",
    )
    parser.add_argument(
        "--backoff-factor",
        type=float,
        default=3.0,
        help="Seconds to wait after a 429 error before retrying (default: 3.0)",
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
    scraper = NinisiteScraper(
        sleep_duration=args.sleep,
        retries=args.retries,
        backoff_factor=args.backoff_factor,
    )

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
        if args.streaming and args.parallel > 1:
            log_message(
                "Warning: --parallel is only effective in non-streaming mode. Ignoring."
            )
        if args.streaming and fmt == "org":
            # Streaming org-mode
            with OrgWriter(args.out) as writer:
                scraper.scrape_discussion_streaming(url, writer, args.paginate)
        else:
            # Buffered modes
            # Fetch first page to get metadata
            log_message("Fetching first page for metadata...")
            first_page_soup = scraper.fetch_page(url)
            if not first_page_soup:
                raise Exception("Could not fetch the first page.")
            metadata = scraper.extract_topic_metadata(first_page_soup)
            # Fetch and process all posts, reusing the first page
            posts = scraper.fetch_and_extract_posts(
                url, parallel=args.parallel, first_page_soup=first_page_soup
            )
            if not posts:
                raise Exception("Could not extract any posts.")
            log_message(f"Extracted a total of {len(posts)} posts.")
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
        traceback.print_exc()
        log_message(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
