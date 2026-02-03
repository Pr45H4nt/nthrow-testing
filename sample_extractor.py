from bs4 import BeautifulSoup
from nthrow.source import SimpleSource
from nthrow.utils import sha1


class MyExtractor(SimpleSource):

    def __init__(self, conn, table, **kwargs):
        super().__init__(conn, table, **kwargs)


    def make_url(self, row, _type):
        args = self.prepare_request_args(row, _type)
        page = args["cursor"] or 1
        return f"https://www.scrapethissite.com/pages/forms/?page_num={page}", page
    

    async def fetch_rows(self, row, _type='to'):
        try:
            url, page = self.make_url(row, _type)
            response = await self.http_get(url)

            if response.status_code == 200:
                rows = []
                content = response.text
                soup = BeautifulSoup(content, "html.parser")

                table_data = soup.find_all(class_ = "team")
                for i , e in enumerate(table_data):
                    data = {
                            "name": e.select_one(".name").get_text(strip=True),
                            "year": e.select_one(".year").get_text(strip=True),
                            "wins": e.select_one(".wins").get_text(strip=True),
                            "losses": e.select_one(".losses").get_text(strip=True),
                            "ot_losses": e.select_one(".ot-losses").get_text(strip=True),
                            "pct": e.select_one(".pct").get_text(strip=True),
                            "gf": e.select_one(".gf").get_text(strip=True),
                            "ga": e.select_one(".ga").get_text(strip=True),
                            "diff": e.select_one(".diff").get_text(strip=True),
                        }
                    unique_id = "".join(data.values())
                    hashed_id = f'https://www.scrapethissite.com#{sha1(unique_id)}'

                    rows.append({
                        'uri': hashed_id,
                        **data
                    })

                return {
                    "rows": [
                        self.make_a_row(
                            row['uri'], self.mini_uri(r["uri"], keep_fragments=True), r
                        )
                        for r in rows
                    ],
                    'state' : {
                        'pagination' : {
                            _type : page + 1
                        }
                    }
                }
            else:
                self.logger.error("Non-200 HTTP response: %s : %s" % (response.status_code, url))
                return self.make_error("HTTP", response.status_code, url)
        except Exception as e:
            self.logger.exception(e)
            return self.make_error("Exception", type(e), str(e))

