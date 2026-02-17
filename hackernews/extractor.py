from bs4 import BeautifulSoup
from nthrow.source import SimpleSource
from nthrow.utils import sha1


class HackerNewsExtractor(SimpleSource):
    def __init__(self, conn, table, **kwargs):
        super().__init__(conn, table, **kwargs)

        # settings
        self.settings["remote"]["limit"] = 30  # HN shows 30 per page
        self.settings["remote"]["refresh_interval"] = 5  
        self.settings["remote"]["refresh_delay"] = 0 

    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        cursor = args["cursor"]  # page number

        if cursor:
            page = int(cursor)
        else:
            page = 1

        if page == 1:
            url = "https://news.ycombinator.com/news"
        else:
            url = f"https://news.ycombinator.com/news?p={page}"

        return url, page

    async def fetch_rows(self, row, _type='to'):

        try:
            url, page = self.make_url(row, _type)
            self.logger.info(f"Fetching HN: {url}")

            response = await self.http_get(url)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")

                # all story rows
                story_rows = soup.select("tr.athing")

                rows = []
                for story in story_rows:
                    story_id = story.get('id')

                    # title and URL
                    title_span = story.select_one(".titleline")
                    if not title_span:
                        continue

                    title_link = title_span.select_one("a")
                    title = title_link.text.strip()
                    link = title_link.get('href', '')

                    meta_row = story.find_next_sibling('tr')
                    if not meta_row:
                        continue

                    score_span = meta_row.select_one(".score")
                    score = int(score_span.text.split()[0]) if score_span else 0

                    user_link = meta_row.select_one(".hnuser")
                    author = user_link.text if user_link else "unknown"

                    age_span = meta_row.select_one(".age")
                    age = age_span.get('title', '') if age_span else ''

                    comments_link = meta_row.select("a")[-1]
                    comments_text = comments_link.text
                    num_comments = 0
                    if "comment" in comments_text:
                        try:
                            num_comments = int(comments_text.split()[0])
                        except:
                            pass

                    data = {
                        "hn_id": story_id,
                        "title": title,
                        "url": link,
                        "score": score,
                        "author": author,
                        "age": age,
                        "num_comments": num_comments,
                    }

                    uri = f"https://news.ycombinator.com/item?id={story_id}"

                    rows.append(
                        self.make_a_row(
                            row['uri'],
                            uri,
                            data
                        )
                    )

                more_link = soup.select_one("a.morelink")
                has_more = more_link is not None
                next_page = str(page + 1) if has_more else None

                self.logger.info(f"Extracted {len(rows)} stories, has_more: {has_more}")

                return {
                    "rows": rows,
                    'state': {
                        'pagination': {
                            _type: next_page
                        }
                    }
                }

            else:
                self.logger.error(f"HTTP {response.status_code}: {url}")
                return self.make_error("HTTP", response.status_code, url)

        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e).__name__, str(e))
