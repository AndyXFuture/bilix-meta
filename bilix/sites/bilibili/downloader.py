import asyncio
import functools
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Union, Sequence, Tuple, List
import aiofiles
import httpx
from datetime import datetime, timedelta
from . import api
from bilix.download.base_downloader_part import BaseDownloaderPart
from bilix._process import SingletonPPE
from bilix.utils import legal_title, cors_slice, valid_sess_data, t2s, json2srt
from bilix.download.utils import req_retry, path_check
from bilix.exception import HandleMethodError, APIUnsupportedError, APIResourceError, APIError
from bilix.cli.assign import kwargs_filter, auto_assemble
from bilix import ffmpeg
import re
import sqlite3 as sql


from danmakuC.bilibili import proto2ass


class DownloaderBilibili(BaseDownloaderPart):
    cookie_domain = "bilibili.com"  # for load cookies quickly
    pattern = re.compile(r"^https?://([A-Za-z0-9-]+\.)*(bilibili\.com|b23\.tv)")

    def __init__(
            self,
            *,
            client: httpx.AsyncClient = None,
            browser: str = None,
            speed_limit: Union[float, int, None] = None,
            stream_retry: int = 5,
            progress=None,
            logger=None,
            part_concurrency: int = 10,
            # unique params
            sess_data: str = None,
            video_concurrency: Union[int, asyncio.Semaphore] = 3,
            hierarchy: bool = True,
    ):
        """

        :param client:
        :param browser:
        :param speed_limit:
        :param stream_retry:
        :param progress:
        :param logger:
        :param sess_data: bilibili SESSDATA cookie
        :param part_concurrency: 媒体分段并发数
        :param video_concurrency: 视频并发数
        :param hierarchy: 是否使用层级目录
        """
        client = client or httpx.AsyncClient(**api.dft_client_settings)
        super(DownloaderBilibili, self).__init__(
            client=client,
            browser=browser,
            speed_limit=speed_limit,
            stream_retry=stream_retry,
            progress=progress,
            logger=logger,
            part_concurrency=part_concurrency,
        )
        client.cookies.set('SESSDATA', valid_sess_data(sess_data))
        self._cate_meta = None
        self.v_sema = asyncio.Semaphore(video_concurrency)
        self.api_sema = asyncio.Semaphore(video_concurrency)
        self.hierarchy = hierarchy
        self.title_overflow = 50

    @classmethod
    def parse_url(cls, url: str):
        if re.match(r'https://space\.bilibili\.com/\d+/favlist\?fid=\d+', url):
            return cls.get_favour
        elif re.match(r'https://space\.bilibili\.com/\d+/channel/seriesdetail\?sid=\d+', url):
            return cls.get_collect_or_list
        elif re.match(r'https://space\.bilibili\.com/\d+/channel/collectiondetail\?sid=\d+', url):
            return cls.get_collect_or_list
        elif re.match(r'https://space\.bilibili\.com/\d+', url):  # up space url
            return cls.get_up
        elif re.search(r'(www\.bilibili\.com)|(b23\.tv)', url):
            return cls.get_video
        raise ValueError(f'{url} no match for bilibili')

    async def get_collect_or_list(self, url, path=Path('.'), people_path=Path("./People/"),
                                  quality=0, image=False, subtitle=False, dm=False, only_audio=False,
                                  codec: str = '', meta=False, update=False):
        """
        下载合集或视频列表
        :cli: short: col
        :param url: 合集或视频列表详情页url
        :param path: 保存路径
        :param quality:
        :param image:
        :param subtitle:
        :param dm:
        :param only_audio:
        :param codec:视频编码
        :param meta: 是否保存meta信息
        :return:
        """
        if 'series' in url:
            list_name, up_name, bvids = await api.get_list_info(self.client, url)
            name = legal_title(f"【视频列表】{up_name}", list_name)
        elif 'collection' in url:
            col_name, up_name, bvids = await api.get_collect_info(self.client, url)
            name = legal_title(f"【合集】{up_name}", col_name)
        else:
            raise ValueError(f'{url} invalid for get_collect_or_list')
        if self.hierarchy:
            path /= name
            path = Path(re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(path)))
            path.mkdir(parents=True, exist_ok=True)
        await asyncio.gather(
            *[self.get_series(f"https://www.bilibili.com/video/{i}", path=path, quality=quality, codec=codec, meta=meta, update=update,
                              image=image, subtitle=subtitle, dm=dm, only_audio=only_audio)
              for i in bvids])

    async def get_favour(self, url_or_fid, path=Path('.'), people_path=Path("./People/"),
                         num=20, keyword='', quality=0, series=True, image=False, subtitle=False,
                         dm=False, only_audio=False, codec: str = '', meta=False, update=False, db=None):
        """
        下载收藏夹内的视频
        :cli: short: fav
        :param url_or_fid: 收藏夹url或收藏夹id
        :param path: 保存路径
        :param num: 下载数量
        :param keyword: 搜索关键词
        :param quality: 画面质量，0为可以观看的最高画质，越大质量越低，超过范围时自动选择最低画质，或者直接使用字符串指定'1080p'等名称
        :param series: 每个视频是否下载所有p，False时仅下载系列中的第一个视频
        :param image: 是否下载封面
        :param subtitle: 是否下载字幕
        :param dm: 是否下载弹幕
        :param only_audio: 是否仅下载音频
        :param codec:视频编码
        :param meta: 是否保存meta信息
        :return:
        """
        if not db is None:
            conn = sql.connect(db)
            cursor = conn.cursor()

        fav_name, up_name, total_size, bvids, _ = await api.get_favour_page_info(self.client, url_or_fid, keyword=keyword)
        if self.hierarchy:
            name = legal_title(f"【收藏夹】{up_name}-{fav_name}")
            path /= name
            path = Path(re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(path)))
            path.mkdir(parents=True, exist_ok=True)
            if not db is None:
                cursor.execute("SELECT * FROM BILIBILI_FAV WHERE name = ?", (name,))
                row = cursor.fetchone()
                # 如果没有找到记录，执行插入操作
                if row is None:
                    cursor.execute("INSERT INTO BILIBILI_FAV (name) VALUES (?)", (name,))
                    conn.commit()
                    cursor.execute("SELECT * FROM BILIBILI_FAV WHERE name = ?", (name,))
                    row = cursor.fetchone()
                fav_id = row[0]
        total = min(total_size, num)
        ps = 20
        page_nums = total // ps + min(1, total % ps)
        cors = []
        for i in range(page_nums):
            if i + 1 == page_nums:
                num = total - (page_nums - 1) * ps
            else:
                num = ps
            cors.append(self._get_favor_by_page(
                url_or_fid, path, i + 1, num, keyword, quality, series, image, subtitle, dm, only_audio, codec=codec, meta=meta, update=update, conn=conn))
        await asyncio.gather(*cors)

    async def _get_favor_by_page(self, url_or_fid, path: Path, pn=1, num=20, keyword='', quality=0,
                                 series=True, image=False, subtitle=False, dm=False, only_audio=False, codec='', meta=False, update=False, conn:sql.connect=None):
        ps = 20
        num = min(ps, num)
        fav_name, up_name, _, bvids, bvnames = await api.get_favour_page_info(self.client, url_or_fid, pn, ps, keyword)

        if not conn is None:
            cursor = conn.cursor()
            conbine_name = f"{up_name}-{fav_name}"
            cursor.execute("SELECT * FROM BILIBILI_FAV WHERE name = ?", (conbine_name,))
            row = cursor.fetchone()
            fav_id = row[0]
            cursor.close()
        bvids = bvids[:num]
        bvnames = bvnames[:num]
        print(bvnames)

        cors = []
        for bv,bvname in zip(bvids, bvnames):
            if ((conn is None) or findFromDb(conn, bv, bvname, "fav_id", fav_id, "BILIBILI_FAV_VIDEO", image=image, subtitle=subtitle, dm=dm, meta=meta)):
                func = self.get_series if series else self.get_video
                # noinspection PyArgumentList
                cors.append(func(f'https://www.bilibili.com/video/{bv}', path=path, quality=quality, codec=codec, meta=meta, update=update,
                                image=image, subtitle=subtitle, dm=dm, only_audio=only_audio))
        await asyncio.gather(*cors)

    @property
    async def cate_meta(self):
        if not self._cate_meta:
            self._cate_meta = asyncio.ensure_future(api.get_cate_meta(self.client))
            self._cate_meta = await self._cate_meta
        elif asyncio.isfuture(self._cate_meta):
            await self._cate_meta
        return self._cate_meta

    async def get_cate(self, cate_name: str, path=Path('.'), people_path=Path("./People/"), num=10, order='click', keyword='', days=7,
                       quality=0, series=True, image=False, subtitle=False, dm=False, only_audio=False, codec='', meta=False, update=False):
        """
        下载分区视频
        :cli: short: cate
        :param cate_name: 分区名称
        :param path: 保存路径
        :param num: 下载数量
        :param order: 何种排序，click播放数，scores评论数，stow收藏数，coin硬币数，dm弹幕数
        :param keyword: 搜索关键词
        :param days: 过去days天中的结果
        :param quality: 画面质量，0为可以观看的最高画质，越大质量越低，超过范围时自动选择最低画质，或者直接使用字符串指定'1080p'等名称
        :param series: 每个视频是否下载所有p，False时仅下载系列中的第一个视频
        :param image: 是否下载封面
        :param subtitle: 是否下载字幕
        :param dm: 是否下载弹幕
        :param only_audio: 是否仅下载音频
        :param codec:视频编码
        :param meta: 是否保存meta信息
        :return:
        """
        cate_meta = await self.cate_meta
        if cate_name not in cate_meta:
            return self.logger.error(f'未找到分区 {cate_name}')
        if 'subChannelId' not in cate_meta[cate_name]:
            sub_names = [i['name'] for i in cate_meta[cate_name]['sub']]
            return self.logger.error(f'{cate_name} 是主分区，仅支持子分区，试试 {sub_names}')
        if self.hierarchy:
            path /= legal_title(f"【分区】{cate_name}")
            path = Path(re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(path)))
            path.mkdir(parents=True, exist_ok=True)
        cate_id = cate_meta[cate_name]['tid']
        time_to = datetime.now()
        time_from = time_to - timedelta(days=days)
        time_from, time_to = time_from.strftime('%Y%m%d'), time_to.strftime('%Y%m%d')
        pagesize = 30
        page = 1
        cors = []
        while num > 0:
            cors.append(self._get_cate_by_page(
                cate_id, path, time_from, time_to, page, min(pagesize, num), order, keyword, quality,
                series, image=image, subtitle=subtitle, dm=dm, only_audio=only_audio, codec=codec, meta=meta, update=update))
            num -= pagesize
            page += 1
        await asyncio.gather(*cors)

    async def _get_cate_by_page(
            self, cate_id, path: Path, time_from, time_to, pn=1, num=30, order='click', keyword='',
            quality=0, series=True, image=False, subtitle=False, dm=False, only_audio=False, codec='', meta=False, update=False):
        bvids = await api.get_cate_page_info(self.client, cate_id, time_from, time_to, pn, 30, order, keyword)
        bvids = bvids[:num]
        func = self.get_series if series else self.get_video
        # noinspection PyArgumentList
        cors = [func(f"https://www.bilibili.com/video/{i}", path=path, quality=quality, codec=codec, meta=meta, update=update,
                     image=image, subtitle=subtitle, dm=dm, only_audio=only_audio)
                for i in bvids]
        await asyncio.gather(*cors)

    async def get_up(
            self, url_or_mid: str, path=Path('.'), people_path=Path("./People/"), num=10, order='pubdate', keyword='', quality=0,
            series=True, image=False, subtitle=False, dm=False, only_audio=False, codec='', meta=False, update=False, db=None):
        """
        下载up主视频
        :cli: short: up
        :param url_or_mid: b站用户空间页面url 或b站用户id，在空间页面的url中可以找到
        :param path: 保存路径
        :param num: 下载总数
        :param order: 何种排序，b站支持：最新发布pubdate，最多播放click，最多收藏stow
        :param keyword: 过滤关键词
        :param quality: 画面质量，0为可以观看的最高画质，越大质量越低，超过范围时自动选择最低画质，或者直接使用字符串指定'1080p'等名称
        :param series: 每个视频是否下载所有p，False时仅下载系列中的第一个视频
        :param image: 是否下载封面
        :param subtitle: 是否下载字幕
        :param dm: 是否下载弹幕
        :param only_audio: 是否仅下载音频
        :param codec:视频编码
        :param meta: 是否保存meta信息
        :return:
        """
        up_info = await api.get_up_info(self.client, url_or_mid)
        print(up_info)
        up_name = up_info.get('name', '')
        print(up_name)
        up_face_url = up_info.get('face', '')
        print(up_face_url)
        up_sign = up_info.get('sign', '')
        print(up_sign)
        up_uid = up_info.get('mid', '')# get('fans_medal').get('medal').get('uid', '')
        print(up_uid)

        if not db is None:
            conn = sql.connect(db)
            cursor = conn.cursor()
        
        

        ps = 30
        media_cors = []
        add_cors = []
        up_name, total_size, bv_ids, bv_names = await api.get_up_video_info(self.client, url_or_mid, 1, ps, order, keyword)
        if self.hierarchy:
            path /= legal_title(f"【up】{up_name}")
            path = Path(re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(path)))
            path.mkdir(parents=True, exist_ok=True)
            if not db is None:
                cursor.execute("SELECT * FROM BILIBILI_UP WHERE name = ?", (up_name,))
                row = cursor.fetchone()
                # 如果没有找到记录，执行插入操作
                if row is None:
                    cursor.execute("INSERT INTO BILIBILI_UP (name) VALUES (?)", (up_name,))
                    conn.commit()
                    cursor.execute("SELECT * FROM BILIBILI_UP WHERE name = ?", (up_name,))
                    row = cursor.fetchone()
                up_id = row[0]
        if meta:
            exist, file_path = path_check(path / f'poster.jpg')
            if not exist and update:
                add_cors.append(self.get_static(up_face_url, path=path / f'poster')) # base_name))
                self.logger.info(f"[cyan]已完成[/cyan] {path / f'poster.jpg'}")
                path_lst, _ = await asyncio.gather(asyncio.gather(*media_cors), asyncio.gather(*add_cors))
            else:
                self.logger.info(f"[green]已存在[/green] {path / f'poster.jpg'}")
        

        num = min(total_size, num)
        page_nums = num // ps + min(1, num % ps)
        cors = []
        for i in range(page_nums):
            if i + 1 == page_nums:
                p_num = num - (page_nums - 1) * ps
            else:
                p_num = ps
            cors.append(self._get_up_by_page(
                url_or_mid, path, i + 1, p_num, order, keyword, quality, series, image=image,
                subtitle=subtitle, dm=dm, only_audio=only_audio, codec=codec, meta=meta, update=update, conn=conn))
        await asyncio.gather(*cors)
        if not db is None:
            conn.commit()
            conn.close()



    async def _get_up_by_page(self, url_or_mid, path: Path, pn=1, num=30, order='pubdate', keyword='', quality=0,
                              series=True, image=False, subtitle=False, dm=False, only_audio=False, codec='', meta=False, update=False, conn:sql.connect=None):
        ps = 30
        num = min(ps, num)
        up_name, _, bvids, bvnames = await api.get_up_video_info(self.client, url_or_mid, pn, ps, order, keyword)
        bvids = bvids[:num]
        bvnames = bvnames[:num]
        print(bvnames)
        
        if not conn is None:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM BILIBILI_UP WHERE name = ?", (up_name,))
            row = cursor.fetchone()
            # 如果没有找到记录，执行插入操作
            if row is None:
                cursor.execute("INSERT INTO BILIBILI_UP (name) VALUES (?)", (up_name,))
                conn.commit()
                cursor.execute("SELECT * FROM BILIBILI_UP WHERE name = ?", (up_name,))
                row = cursor.fetchone()
            up_id = row[0]
            cursor.close()
        
        func = self.get_series if series else self.get_video
        # noinspection PyArgumentList
        await asyncio.gather(
            *[func(f'https://www.bilibili.com/video/{bv}', path=path, quality=quality, codec=codec, meta=meta, update=update,
                   image=image, subtitle=subtitle, dm=dm, only_audio=only_audio) 
            for bv,bvname in zip(bvids, bvnames) 
            if (conn is None) or findFromDb(conn, bv, bvname, "up_id", up_id, "BILIBILI_UP_VIDEO", image=image, subtitle=subtitle, dm=dm, meta=meta) ])
        

            

    async def get_series(self, url: str, path=Path('.'), people_path=Path("./People/"),
                         quality: Union[str, int] = 0, image=False, subtitle=False,
                         dm=False, only_audio=False, p_range: Sequence[int] = None, codec: str = '', meta=False, update=False):
        """
        下载某个系列（包括up发布的多p投稿，动画，电视剧，电影等）的所有视频。只有一个视频的情况下仍然可用该方法
        :cli: short: s
        :param url: 系列中任意一个视频的url
        :param path: 保存路径
        :param quality: 画面质量，0为可以观看的最高画质，越大质量越低，超过范围时自动选择最低画质，或者直接使用字符串指定'1080p'等名称
        :param image: 是否下载封面
        :param subtitle: 是否下载字幕
        :param dm: 是否下载弹幕
        :param only_audio: 是否仅下载音频
        :param p_range: 下载集数范围，例如(1, 3)：P1至P3
        :param codec: 视频编码（可通过info获取）
        :param meta: 是否保存meta信息
        :return:
        """
        try:
            async with self.api_sema:
                video_info = await api.get_video_info(self.client, url)
        except (APIResourceError, APIUnsupportedError) as e:
            return self.logger.warning(e)
        # print(video_info)
        try:
            # todo: 已知问题：节日视频似乎没有pages，遇到节日视频暂时跳过
            pages = video_info.pages
        except:
            pages = 0
            return
            
        if self.hierarchy and len(pages) > 1:
            path /= video_info.title
            path = Path(re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(path)))
            path.mkdir(parents=True, exist_ok=True)
            add_cors = []
            media_cors = []
            if meta:
                exist, file_path = path_check(path / f'poster.jpg')
                if not exist and update:
                    add_cors.append(self.get_static(video_info.img_url, path=path / f'poster')) # base_name))
                    path_lst, _ = await asyncio.gather(asyncio.gather(*media_cors), asyncio.gather(*add_cors))
                    self.logger.info(f"[cyan]已完成[/cyan] {path / f'poster.jpg'}")
                else:
                    self.logger.info(f"[green]已存在[/green] {path / f'poster.jpg'}")
        
        cors = [self.get_video(p.p_url, path=path,
                               quality=quality, image=image, subtitle=subtitle, dm=dm,
                               only_audio=only_audio, codec=codec, meta=meta, update=update,
                               video_info=video_info if idx == video_info.p else None)
                for idx, p in enumerate(pages)]
        if p_range:
            cors = cors_slice(cors, p_range)
        await asyncio.gather(*cors)

    async def get_video(self, url: str, path=Path('.'), people_path=Path("./People/"), 
                        quality: Union[str, int] = 0, image=False, subtitle=False, dm=False, only_audio=False,
                        codec: str = '', meta=False, time_range: Tuple[int, int] = None, video_info: api.VideoInfo = None, update=False):
        """
        下载单个视频
        :cli: short: v
        :param url: 视频的url
        :param path: 保存路径
        :param quality: 画面质量，0为可以观看的最高画质，越大质量越低，超过范围时自动选择最低画质，或者直接使用字符串指定'1080p'等名称
        :param image: 是否下载封面
        :param subtitle: 是否下载字幕
        :param dm: 是否下载弹幕
        :param only_audio: 是否仅下载音频
        :param codec: 视频编码（可通过codec获取）
        :param meta: 是否保存meta信息
        :param time_range: 切片的时间范围
        :param video_info: 额外数据，提供时不用再次请求页面
        :return:
        """
        async with self.v_sema:
            if not video_info:
                try:
                    video_info = await api.get_video_info(self.client, url)
                except (APIResourceError, APIUnsupportedError) as e:
                    return self.logger.warning(e)
            # print( video_info )
            
            p_name = legal_title(video_info.pages[video_info.p].p_name)
            task_name = legal_title(video_info.title, p_name)
            # if title is too long, use p_name as base_name
            base_name = p_name if len(video_info.title) > self.title_overflow and self.hierarchy and p_name else \
                task_name
            media_name = base_name if not time_range else legal_title(base_name, *map(t2s, time_range))
            bv_id = legal_title(video_info.bvid, p_name)
            print( bv_id )
            media_cors = []
            task_id = await self.progress.add_task(total=None, description=task_name)
            if video_info.dash:
                try:  # choose video quality
                    video, audio = video_info.dash.choose_quality(quality, codec)
                except KeyError:
                    self.logger.warning(
                        f"{task_name} 清晰度<{quality}> 编码<{codec}>不可用，请检查输入是否正确或是否需要大会员")
                else:
                    # 该方法只支持单个字符
                    # table = str.maketrans({"S0": "S·0", "S1": "S·1", "S2": "S·2", "S3": "S·3", "S4": "S·4", "S5": "S·5", "S6": "S·6", "S7": "S·7", "S8": "S·8", "S9": "S·9", \
                    #                        "E0": "E·0", "E1": "E·1", "E2": "E·2", "E3": "E·3", "E4": "E·4", "E5": "E·5", "E6": "E·6", "E7": "E·7", "E8": "E·8", "E9": "E·9"})
                    # video_name = media_name.translate(table)
                    
                    # 该方法可以支持多个字符
                    video_name = media_name
                    video_name = video_name.replace("S0", "S·0")
                    video_name = video_name.replace("S1", "S·1")
                    video_name = video_name.replace("S2", "S·2")
                    video_name = video_name.replace("S3", "S·3")
                    video_name = video_name.replace("S4", "S·4")
                    video_name = video_name.replace("S5", "S·5")
                    video_name = video_name.replace("S6", "S·6")
                    video_name = video_name.replace("S7", "S·7")
                    video_name = video_name.replace("S8", "S·8")
                    video_name = video_name.replace("S9", "S·9")
                    video_name = video_name.replace("E0", "E·0")
                    video_name = video_name.replace("E1", "E·1")
                    video_name = video_name.replace("E2", "E·2")
                    video_name = video_name.replace("E3", "E·3")
                    video_name = video_name.replace("E4", "E·4")
                    video_name = video_name.replace("E5", "E·5")
                    video_name = video_name.replace("E6", "E·6")
                    video_name = video_name.replace("E7", "E·7")
                    video_name = video_name.replace("E8", "E·8")
                    video_name = video_name.replace("E9", "E·9")
                    path = path / f'{video_name} - {video_info.bvid}'          # 每个视频单独文件夹存放
                    path = Path(re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(path)))
                    path.mkdir(exist_ok=True)
                    tmp: List[Tuple[api.Media, Path]] = []
                    # 1. only video
                    if not audio and not only_audio:
                        tmp.append((video, path / f'{bv_id}.mp4'))
                    # 2. video and audio
                    elif audio and not only_audio:
                        exists, media_path = path_check(path / f'{bv_id}.mp4')
                        if exists:
                            self.logger.info(f'[green]已存在[/green] {media_path}') # {media_path.name}')
                        else:
                            tmp.append((video, path / f'{bv_id}-v'))
                            tmp.append((audio, path / f'{bv_id}-a'))
                            # task need to be merged
                            await self.progress.update(task_id=task_id, upper=ffmpeg.combine)
                    # 3. only audio
                    elif audio and only_audio:
                        tmp.append((audio, path / f'{bv_id}{audio.suffix}'))
                    else:
                        self.logger.warning(f"No audio for {task_name}")
                    # convert to coroutines
                    if not time_range:
                        media_cors.extend(self.get_file(t[0].urls, path=t[1], task_id=task_id) for t in tmp)
                    else:
                        if len(tmp) > 0:
                            fut = asyncio.Future()  # to fix key frame
                            v = tmp[0]
                            media_cors.append(self.get_media_clip(v[0].urls, v[1], time_range,
                                                                  init_range=v[0].segment_base['initialization'],
                                                                  seg_range=v[0].segment_base['index_range'],
                                                                  set_s=fut,
                                                                  task_id=task_id))
                        if len(tmp) > 1:  # with audio
                            a = tmp[1]
                            media_cors.append(self.get_media_clip(a[0].urls, a[1], time_range,
                                                                  init_range=a[0].segment_base['initialization'],
                                                                  seg_range=a[0].segment_base['index_range'],
                                                                  get_s=fut,
                                                                  task_id=task_id))

            elif video_info.other:
                self.logger.warning(
                    f"{task_name} 未解析到dash资源，转入durl mp4/flv下载（不需要会员的电影/番剧预览，不支持dash的视频）")
                media_name = base_name
                if len(video_info.other) == 1:
                    m = video_info.other[0]
                    media_cors.append(
                        self.get_file(m.urls, path=path / f'{bv_id}.{m.suffix}', task_id=task_id))
                else:
                    exist, media_path = path_check(path / f'{bv_id}.mp4')
                    if exist:
                        self.logger.info(f'[green]已存在[/green] {media_path}')# .name}')
                    else:
                        p_sema = asyncio.Semaphore(self.part_concurrency)

                        async def _get_file(media: api.Media, p: Path) -> Path:
                            async with p_sema:
                                return await self.get_file(media.urls, path=p, task_id=task_id)

                        for i, m in enumerate(video_info.other):
                            f = f'{bv_id}-{i}.{m.suffix}'
                            media_cors.append(_get_file(m, path / f))
                        await self.progress.update(task_id=task_id, upper=ffmpeg.concat)
            else:
                self.logger.warning(f'{task_name} 需要大会员或该地区不支持')
            # additional task
            add_cors = []
            if image or subtitle or dm or meta:
                extra_path = path
                if image:
                    add_cors.append(self.get_static(video_info.img_url, path=extra_path / f'{bv_id}-fanart')) # base_name))
                    add_cors.append(self.get_static(video_info.img_url, path=extra_path / f'{bv_id}-backdrop1')) # base_name))
                if subtitle:
                    add_cors.append(self.get_subtitle(url, path=extra_path, video_info=video_info))
                if dm:
                    try:
                        width, height = video.width, video.height
                    except UnboundLocalError:
                        width, height = 1920, 1080
                    add_cors.append(self.get_dm(
                        url, path=extra_path, convert_func=self._dm2ass_factory(width, height), video_info=video_info, update=update))
                if meta:
                    add_cors.append(self.get_meta_nfo(url, path=extra_path, video_info=video_info, bv_id=bv_id))
                    # with open(extra_path / f'{bv_id}.json', 'w', encoding='utf-8') as f:
                    #     f.write(video_info.model_dump_json(exclude={"dash", "other"}))
                    # self.logger.info(f'{bv_id}.json')
            path_lst, _ = await asyncio.gather(asyncio.gather(*media_cors), asyncio.gather(*add_cors))

        if upper := self.progress.tasks[task_id].fields.get('upper', None):
            await upper(path_lst, media_path)
            self.logger.info(f'[cyan]已完成[/cyan] {media_path}')# .name}')
        await self.progress.update(task_id, visible=False)

    @staticmethod
    def _dm2ass_factory(width: int, height: int):
        async def dm2ass(protobuf_bytes: bytes) -> bytes:
            loop = asyncio.get_event_loop()
            f = functools.partial(proto2ass, protobuf_bytes, width, height, font_size=width / 50, alpha=0.5)    # 设置弹幕参数，具体参考danmakuC的__main__.py
            content = await loop.run_in_executor(SingletonPPE(), f)
            return content.encode('utf-8')

        return dm2ass

    async def get_dm(self, url, path=Path('.'), people_path=Path("./People/"), update=False, convert_func=None, video_info=None):
        """
        下载视频的弹幕
        :cli: short: dm
        :param url: 视频url
        :param path: 保存路径
        :param update: 是否更新覆盖之前下载的弹幕文件
        :param convert_func:
        :param video_info: 额外数据，提供则不再访问前端
        :return:
        """
        if not video_info:
            video_info = await api.get_video_info(self.client, url)
        aid, cid = video_info.aid, video_info.cid
        file_type = '.' + ('pb' if not convert_func else convert_func.__name__.split('2')[-1])
        p_name = video_info.pages[video_info.p].p_name
        # to avoid file name too long bug
        # if len(video_info.title) > self.title_overflow and self.hierarchy and p_name:
        #     file_name = legal_title(p_name, "弹幕") + file_type
        # else:
        # print(p_name)
        file_name = legal_title(video_info.bvid, p_name, "弹幕.zh", join_str = ".") + file_type
        file_path = path / file_name
        exist, file_path = path_check(file_path)
        if not update and exist:
            self.logger.info(f"[green]已存在[/green] {file_name}")
            return file_path
        dm_urls = await api.get_dm_urls(self.client, aid, cid)
        cors = [req_retry(self.client, dm_url) for dm_url in dm_urls]
        results = await asyncio.gather(*cors)
        content = b''.join(res.content for res in results)
        content = convert_func(content) if convert_func else content
        if asyncio.iscoroutine(content):
            content = await content
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(content)
        self.logger.info(f"[cyan]已完成[/cyan] {file_name}")
        return file_path

    async def get_subtitle(self, url, path=Path('.'), people_path=Path("./People/"), convert_func=json2srt, video_info=None):
        """
        下载视频的字幕文件
        :cli: short: sub
        :param url: 视频url
        :param path: 字幕文件保存路径
        :param convert_func: function used to convert original subtitle text
        :param video_info: 额外数据，提供则不再访问前端
        :return:
        """
        if not video_info:
            video_info = await api.get_video_info(self.client, url)
        p, cid = video_info.p, video_info.cid
        p_name = video_info.pages[p].p_name
        try:
            subtitles = await api.get_subtitle_info(self.client, video_info.bvid, cid)
        except APIError as e:
            return self.logger.warning(e)
        cors = []

        for sub_url, sub_name in subtitles:
            # if len(video_info.title) > self.title_overflow and self.hierarchy and p_name:
            #     file_name = legal_title(p_name, sub_name)
            # else:
            file_name = legal_title(video_info.bvid, p_name, sub_name, "zh", join_str = ".")
            cors.append(self.get_static(sub_url, path / file_name, convert_func=convert_func))
        paths = await asyncio.gather(*cors)
        return paths
    
    async def get_meta_nfo(self, url, path=Path('.'), people_path=Path("./People/"), video_info=None, bv_id=None, update=False):
        """
        提取生成nfo文件
        :cli: short: sub
        :param url: 视频url
        :param path: 文件保存路径
        :param video_info: 额外数据，提供则不再访问前端
        :return:
        """
        if not video_info:
            video_info = await api.get_video_info(self.client, url)
            bv_id = legal_title(video_info.bvid)
        p, cid = video_info.p, video_info.cid
        p_name = video_info.pages[p].p_name

        exist_path = path / f'{legal_title(bv_id)}.nfo'# , p_name)}.nfo'
        exist, exist_path = path_check(exist_path)
        if not update and exist:
            self.logger.info(f"[green]已存在[/green] {exist_path}")
            return exist_path
        video_user_info = await api._get_video_info_from_api(self.client, url)
        # print(video_user_info)
        pubdate_timestamp = video_user_info.get('data').get('pubdate','0')
        pubdate_date = datetime.fromtimestamp(pubdate_timestamp)
        ctime_timestamp = video_user_info.get('data').get('ctime','0')
        ctime_date = datetime.fromtimestamp(ctime_timestamp)
        
        root = ET.Element("movie")
        ET.SubElement(root, "plot").text = video_info.desc
        ET.SubElement(root, "title").text = legal_title(video_info.title, p_name)
        ET.SubElement(root, "trailer").text = f"{url}"
        pubdateText = datetime(pubdate_date.year, pubdate_date.month, pubdate_date.day).strftime('%Y-%m-%d')
        ctimeText = datetime(ctime_date.year, ctime_date.month, ctime_date.day).strftime('%Y-%m-%d')
        ET.SubElement(root, "premiered").text = f'{pubdateText}'
        ET.SubElement(root, "releasedate").text = f'{ctimeText}'
        ET.SubElement(root, "aired").text = f'{pubdateText}'
        ET.SubElement(root, "year").text = f'{pubdate_date.year}'
        ET.SubElement(root, "mpaa").text = "PG"
        ET.SubElement(root, "customrating").text = "CN"
        ET.SubElement(root, "country").text = "中国"
        ET.SubElement(root, "runtime").text = f"{video_user_info.get('data').get('duration','')}秒"
        ET.SubElement(root, "id").text = video_info.bvid
        ET.SubElement(root, "num").text = video_info.bvid
        ET.SubElement(root, "genre").text = video_user_info.get('data').get('tname','')
        ET.SubElement(root, "studio").text = f"bilibili"

        # 获取tag信息
        tags = video_info.tags
        tag = [None]*len(tags)
        # print(video_info.tags)
        for index, member in enumerate(tags):
            # print(index)
            # print(member)
            tag[index] = ET.SubElement(root, "tag")
            tag[index].text = f"{member}"
        
        # 获取演员信息
        staff = video_user_info.get('data').get('staff','')
        if staff == '':
            owner = video_user_info.get('data').get('owner','')
            actor = ET.SubElement(root, "actor")
            ET.SubElement(actor, "name").text = f"{owner.get('name','')}"
            ET.SubElement(actor, "mid").text = f"{owner.get('mid','')}"
            ET.SubElement(actor, "type").text = f"UP主"
            ET.SubElement(actor, "sortorder").text = f"0"
            ET.SubElement(actor, "thumb").text = f"/nfo/People/{owner.get('name','')[:1]}/{owner.get('name','')}/folder.jpg"
            # print(f"/nfo/People/{owner.get('name','')[:1]}/{owner.get('name','')}/folder.jpg")
            file_path = Path(people_path, f"{owner.get('name','')[:1]}/{owner.get('name','')}")
            file_path.mkdir(parents=True, exist_ok=True)
            file_name = file_path / f"folder.jpg"
            # print(file_name)
            path1 = "."
            exist, file_name = path_check(file_name)
            if not update and exist:
                self.logger.info(f"[green]已存在[/green] {file_name}")
            else:
                media_cors = []
                add_cors = []
                pic_url = owner.get('face','')
                add_cors.append(self.get_static(pic_url, path=file_path / f"folder")) # base_name))
                path_lst, _ = await asyncio.gather(asyncio.gather(*media_cors), asyncio.gather(*add_cors))
                self.logger.info(f"[cyan]已完成[/cyan] {file_name}")
        else:
            actor = [None]*len(staff)
            for index, member in enumerate(staff):
                # print(index)
                # print(member)
                actor[index] = ET.SubElement(root, "actor")
                ET.SubElement(actor[index], "name").text = f"{member.get('name','')}"
                ET.SubElement(actor[index], "mid").text = f"{member.get('mid','')}"
                ET.SubElement(actor[index], "role").text = f"{member.get('title','')}"
                ET.SubElement(actor[index], "type").text = f"Producer"
                ET.SubElement(actor[index], "sortorder").text = f"{index}"
                ET.SubElement(actor[index], "thumb").text = f"/nfo/People/{member.get('name','')[:1]}/{member.get('name','')}/folder.jpg"
                # print(f"/nfo/People/{member.get('name','')[:1]}/{member.get('name','')}/folder.jpg")
                # todo: 检查演员文件夹是否有该UP主的信息
                file_path = Path(people_path, f"{member.get('name','')[:1]}/{member.get('name','')}")
                file_path.mkdir(parents=True, exist_ok=True)
                file_name = file_path / f"folder.jpg"
                # print(file_name)
                path1 = "."
                exist, file_name = path_check(file_name)
                if not update and exist:
                    self.logger.info(f"[green]已存在[/green] {file_name}")
                else:
                    media_cors = []
                    add_cors = []
                    pic_url = member.get('face','')
                    add_cors.append(self.get_static(pic_url, path=file_path / f"folder")) # base_name))
                    path_lst, _ = await asyncio.gather(asyncio.gather(*media_cors), asyncio.gather(*add_cors))
                    self.logger.info(f"[cyan]已完成[/cyan] {file_name}")

        
        # 存入nfo文件
        tree = ET.ElementTree(root)
        file_name = legal_title(bv_id)#, p_name)
        file_path = path / f'{file_name}.nfo'
        with open(file_path, 'w', encoding='utf-8') as f:
            tree.write(file_path, encoding="utf-8", xml_declaration=True)
        self.logger.info(f"[cyan]已完成[/cyan] {file_path}")
        return file_path
    
    @classmethod
    @auto_assemble
    def handle(cls, method: str, keys: Tuple[str, ...], options: dict):
        if cls.pattern.match(keys[0]) or method == 'cate' or method == 'get_cate':
            if method in {'auto', 'a'}:
                m = cls.parse_url(keys[0])
            elif method in cls._cli_map:
                m = cls._cli_map[method]
            else:
                raise HandleMethodError(cls, method=method)
            d = cls(sess_data=options['cookie'], **kwargs_filter(cls, options))
            return d, m

def findFromDb( conn:sql.connect, bvid, bvname, pid_name, pid, tableName, image=False, subtitle=False, dm=False, meta=False, update=False):
    if conn is None:
        return 1
    if update:
        return 1
    
    cursor = conn.cursor() 

    bvname = bvname.replace("S0", "S·0")
    bvname = bvname.replace("S1", "S·1")
    bvname = bvname.replace("S2", "S·2")
    bvname = bvname.replace("S3", "S·3")
    bvname = bvname.replace("S4", "S·4")
    bvname = bvname.replace("S5", "S·5")
    bvname = bvname.replace("S6", "S·6")
    bvname = bvname.replace("S7", "S·7")
    bvname = bvname.replace("S8", "S·8")
    bvname = bvname.replace("S9", "S·9")
    bvname = bvname.replace("E0", "E·0")
    bvname = bvname.replace("E1", "E·1")
    bvname = bvname.replace("E2", "E·2")
    bvname = bvname.replace("E3", "E·3")
    bvname = bvname.replace("E4", "E·4")
    bvname = bvname.replace("E5", "E·5")
    bvname = bvname.replace("E6", "E·6")
    bvname = bvname.replace("E7", "E·7")
    bvname = bvname.replace("E8", "E·8")
    bvname = bvname.replace("E9", "E·9")
    bvname = re.sub('[\.\:\*\?\"\<\>\|]', str('_'), str(bvname))
        
    haveVideo = 0
    haveCover = 0
    haveSubtitle = 0
    haveDm = 0
    haveMeta = 0
    
    cursor.execute(f"SELECT video, cover, subtitle, dm, meta FROM {tableName} WHERE bvid = ? AND name = ? AND {pid_name} = ?", (bvid, bvname, pid,))
    row = cursor.fetchone()
    if row is None:
        cursor.close()
        return 1
    haveVideo, haveCover, haveSubtitle, haveDm, haveMeta = row
    cursor.close()

    if not haveVideo:
        return 1
    if image==True and (not haveCover):
        return 1
    if subtitle==True and (not haveSubtitle):
        return 1
    if dm==True and (not haveDm):
        return 1
    if meta==True and (not haveMeta):
        return 1
    
    print(f"已存在={bvid}=")
    return 0