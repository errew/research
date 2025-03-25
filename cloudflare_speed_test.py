#!/usr/bin/env python3
import os
import re
import csv
import time
import socket
import ipaddress
import asyncio
import aiohttp
from aiohttp import ClientTimeout
import ssl
from pathlib import Path
from typing import List, Dict, Tuple, Any
from dataclasses import dataclass
import argparse
import random

@dataclass
class IPTestResult:
    ip: str
    tcp_delay: float
    http_delay: float
    download_speed: float
    location: str = ''
    colo: str = ''

class CloudflareSpeedTest:
    def __init__(self, args):
        self.ip_file = args.ip_file
        self.test_times = args.test_times
        self.download_test_count = args.download_count
        self.download_test_timeout = args.timeout
        self.test_url = args.test_url
        self.tcp_port = 443  # 默认端口
        self.location_map = self._load_location_map()
        self.concurrency = args.concurrency  # 直接初始化

    def _load_location_map(self) -> Dict[str, str]:
        """加载IP地理位置映射关系"""
        location_map = {}
        try:
            with open('colo.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    # 使用正则匹配机场代码
                    match = re.search(r'\((\w{3})\)$', line.strip())
                    if match:
                        city = line.split(',')[0].strip()
                        region = match.group(1)
                    else:
                        continue
                    location_map[region] = city
        except Exception as e:
            print(f'加载地理位置信息失败：{str(e)}')
        return location_map

    def _load_ip_list(self) -> List[str]:
        """从ip.txt加载IP列表"""
        ip_list = []
        try:
            with open(self.ip_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    # 处理CIDR格式的IP段
                    if '/' in line:
                        ip_list.extend(self._expand_cidr(line))
                    else:
                        ip_list.append(line)
        except Exception as e:
            print(f'加载IP列表失败：{str(e)}')
            return []
        # 总数限制
        if len(ip_list) > 20000:
            ip_list = random.sample(ip_list, 20000)
        return ip_list

    def _expand_cidr(self, cidr: str) -> List[str]:
        """展开CIDR格式的IP段"""
        import random
        try:
            network = ipaddress.ip_network(cidr, strict=False)
            prefix = network.prefixlen
            hosts = list(network.hosts())
            
            # 对掩码≤24的大段进行随机采样
            if prefix <= 24 and len(hosts) > 5:
                return [str(host) for host in random.sample(hosts, 5)]
            return [str(host) for host in hosts]
        except ValueError:
            print(f"无效的 CIDR: {cidr}")
            return []

    async def test_tcp_delay(self, ip: str) -> float:
        """测试TCP连接延迟"""
        delays = []
        for _ in range(self.test_times):
            try:
                start_time = time.time()
                reader, writer = await asyncio.open_connection(ip, self.tcp_port)
                delay = (time.time() - start_time) * 1000
                writer.close()
                await writer.wait_closed()
                delays.append(delay)
            except (asyncio.TimeoutError, socket.gaierror, ConnectionRefusedError, OSError) as e:
                print(f"TCP测试异常 (IP: {ip}): {type(e).__name__} - {str(e)}")
                continue
        return min(delays) if delays else float('inf')

    async def test_http_delay(self, ip: str) -> Tuple[float, str]:
        """测试HTTP请求延迟并获取机房信息"""
        delays = []
        colo = ''
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        async with aiohttp.ClientSession() as session:
            for _ in range(self.test_times):
                try:
                    start_time = time.time()
                    async with session.head(self.test_url, headers=headers, timeout=ClientTimeout(total=2)) as resp:  # Use ClientTimeout
                        delay = (time.time() - start_time) * 1000
                        if not colo and 'cf-ray' in resp.headers:
                            cf_ray = resp.headers['cf-ray']
                            match = re.search(r'-([A-Z]{3})[0-9]*$', cf_ray)
                            if match:
                                colo = match.group(1)
                        delays.append(delay)
                except Exception as e:
                    print(f"HTTP测试异常 (IP: {ip}): {type(e).__name__} - {str(e)}")
                    continue
        return (min(delays) if delays else float('inf')), colo

    async def test_download_speed(self, ip: str) -> float:
        """测试下载速度"""
        session = None
        try:
            # 从URL中提取域名
            host = self.test_url.split('/')[2]
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Host': host  # 保持原始域名作为Host头
            }
            print(f"\n[DEBUG] 解析配置 - 目标IP: {ip}, 原始Host头: {host}")
            start_time = time.time()
            total_size = 0

            # 动态导入 AbstractResolver, ResolveResult
            try:
                from aiohttp.abc import AbstractResolver, ResolveResult
            except ImportError:
                from aiohttp.resolver import AbstractResolver
                from aiohttp.typedefs import ResolveResult

            class CustomResolver(AbstractResolver):
                def __init__(self, target_ip):
                    self.target_ip = target_ip

                async def resolve(self, host: str, port: int = 0, family: int = socket.AF_INET) ->  List[ResolveResult]:
                    return [ResolveResult(
                        hostname=host,
                        host=self.target_ip,
                        port=port,
                        family=family,
                        proto=socket.IPPROTO_TCP,
                        flags=0,
                    )]

                async def close(self):
                    pass

            resolver = CustomResolver(ip)
            conn = aiohttp.TCPConnector(resolver=resolver, limit_per_host=2)

            session = aiohttp.ClientSession(connector=conn)
            print(f"开始下载测速 (IP: {ip}, Host: {host})")
            try:
                # 使用原始URL，但通过自定义解析器将域名解析为指定IP
                print(f"[DEBUG] 创建自定义解析器 - 强制解析 {host} -> {ip}")
                ssl_context = ssl.create_default_context()
                print("[DEBUG] 启用完整SSL证书验证")
                timeout = ClientTimeout(total=self.download_test_timeout, connect=5, sock_read=self.download_test_timeout)
                
                async with session.get(self.test_url, headers=headers, timeout=timeout, ssl=ssl_context) as resp:
                    print(f"[DEBUG] 响应状态码: {resp.status}, 响应头: {dict(resp.headers)}")
                    if resp.status != 200:
                        print(f"下载测速状态码错误 (IP: {ip}): {resp.status}")
                        return 0.0

                    buffer_size = 1024 * 16
                    chunk_counter = 0
                    read_task = asyncio.create_task(resp.content.readany())

                    while True:
                        chunk = await asyncio.wait_for(read_task, timeout=self.download_test_timeout)
                        if not chunk:
                            break
                        chunk_counter += 1
                        total_size += len(chunk)
                        if chunk_counter % 10 == 0:
                            print(f"[进度] 已接收 {chunk_counter} 个数据块，累计 {total_size/1024:.2f}KB")
                        read_task = asyncio.create_task(resp.content.readany())

                duration = time.time() - start_time
                if duration > 0:
                    # 计算下载速度（MB/s）
                    return total_size / duration / 1024 / 1024  # MB/s
                return 0.0

            except Exception as e:
                print(f"下载测速时出错 (IP: {ip}): {str(e)}")
                import traceback
                print(f"[DEBUG] 错误堆栈:\n{traceback.format_exc()}")
                return 0.0

            finally:
                # 显式关闭连接器和会话
                try:
                    if 'conn' in locals() and conn and not conn.closed:
                        await conn.close()
                    if 'session' in locals() and session and not session.closed:
                        await session.close()
                except Exception as e:
                    print(f'资源关闭异常: {str(e)}')

        except Exception as e:
            print(f"测试IP {ip} 时发生错误: {str(e)}")
            return 0.0
        finally:
            # 确保资源释放（二次检查，防止任何情况下的资源泄漏）
            try:
                if 'conn' in locals() and conn and not getattr(conn, 'closed', False):
                    await conn.close()
                if 'session' in locals() and session and not getattr(session, 'closed', False):
                    await session.close()
            except Exception as e:
                print(f'最终资源关闭异常: {str(e)}')
            # 计算并返回速度（如果duration已定义）
            if 'duration' in locals() and duration > 0:
                # 保持与上面计算逻辑一致（MB/s）
                speed = total_size / duration / 1024 / 1024  # MB/s
                return speed
            return 0.0

    async def test_ip(self, ip: str) -> IPTestResult:
        """测试单个IP的综合性能"""
        tcp_delay = await self.test_tcp_delay(ip)
        if tcp_delay == float('inf'):
            return IPTestResult(ip, tcp_delay, float('inf'), 0.0)
        
        http_delay, colo = await self.test_http_delay(ip)
        if http_delay == float('inf'):
            return IPTestResult(ip, tcp_delay, http_delay, 0.0)
        
        download_speed = await self.test_download_speed(ip)
        location = self.location_map.get(colo, '')
        
        return IPTestResult(ip, tcp_delay, http_delay, download_speed, location, colo)

    async def run_test(self):
        """运行测速程序"""
        try:
            print('开始 CloudflareSpeedTest 测速...')
            ip_list = self._load_ip_list()
            if not ip_list:
                print('IP列表为空，请检查ip.txt文件！')
                return
            
            print(f'成功加载 {len(ip_list)} 个待测试IP')
            
            results = []
            total = len(ip_list)
            completed = 0

            # 创建信号量限制并发数
            sem = asyncio.Semaphore(self.concurrency)

            async def test_ip_with_progress(ip):
                async with sem:
                    nonlocal completed
                    try:
                        result = await self.test_ip(ip)
                        if result.tcp_delay != float('inf'):
                            results.append(result)
                            progress_percent = (completed / total) * 100
                            print(f'[{completed}/{total} - {progress_percent:.1f}%] 测试IP: {ip}')
                            print(f'  TCP延迟: {result.tcp_delay:.2f}ms')
                            print(f'  HTTP延迟: {result.http_delay:.2f}ms')
                            print(f'  下载速度: {result.download_speed:.2f}MB/s')
                            if result.location:
                                print(f'  地区: {result.location} ({result.colo})')
                            print('-------------------')
                    except Exception as e:
                        print(f'测试IP {ip} 时发生错误: {str(e)}')
                    completed += 1

            # 并发测试所有IP
            tasks = [test_ip_with_progress(ip) for ip in ip_list]
            await asyncio.gather(*tasks)
            
            # 优先按照下载速度排序，其次考虑延迟
            results.sort(key=lambda x: (-x.download_speed, x.tcp_delay))
            results = results[:500]  # 保留前500个结果
            
            # 保存测试结果
            try:
                print('正在保存测试结果...')
                self._save_results(results)
                print('结果文件已成功写入')
            except Exception as e:
                print(f'文件保存失败：{str(e)}')
            
            if results:
                best = results[0]
                print(f'\n最优IP: {best.ip}')
                print(f'TCP延迟: {best.tcp_delay:.2f}ms')
                print(f'HTTP延迟: {best.http_delay:.2f}ms')
                print(f'下载速度: {best.download_speed:.2f}MB/s')
                if best.location:
                    print(f'地区: {best.location} ({best.colo})')
        finally:
            # 清理所有未完成任务
            current_task = asyncio.current_task()
            tasks = [t for t in asyncio.all_tasks() if t is not current_task]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

            # 所有资源已在各测试方法中释放，无需额外清理
            pass

    def _save_results(self, results: List[IPTestResult]):
        """保存测试结果到CSV文件"""
        try:
            if len(results) == 0:
                print('警告：没有符合条件的结果需要保存！')
                return
                
            try:
                save_path = os.path.abspath('result_hosts.txt')
                print(f'准备保存{len(results)}条结果到：{save_path}')
                
                with open(save_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(['IP', 'TCP延迟', 'HTTP延迟', '下载速度', '地区', '数据中心'])
                    for result in results:
                        writer.writerow([
                            result.ip,
                            f'{result.tcp_delay:.2f}',
                            f'{result.http_delay:.2f}',
                            f'{result.download_speed:.2f}',
                            result.location,
                            result.colo
                        ])
                print('结果已保存到 result_hosts.txt')
            except IOError as e:
                print(f'文件写入失败：{str(e)}')
                print(f'当前工作目录：{os.getcwd()}')
                print(f'请检查写入权限和磁盘空间')
            except Exception as e:
                print(f'发生未知错误：{str(e)}')
        finally:
            pass







    parser = argparse.ArgumentParser(description='CloudflareSpeedTest 参数配置')
    parser.add_argument('-f', '--ip-file', default='ip.txt', help='IP列表文件路径 (默认: ip.txt)')
    parser.add_argument('--test-times', type=int, default=4, help='TCP/HTTP测试次数 (默认: 4)')
    parser.add_argument('--download-count', type=int, default=10, help='下载测速IP数量 (默认: 10)')
    parser.add_argument('--timeout', type=int, default=10, help='下载测速超时时间(秒) (默认: 2)')
    parser.add_argument('--test-url', default='https://speed.cloudflare.com/__down?bytes=200000000', help='测速用的URL地址')
    parser.add_argument('--concurrency', type=int, default=50, help='并发连接数 (默认: 50)')
    return parser.parse_args()



def main():
    args = parse_args()
    speed_test = CloudflareSpeedTest(args)
    # Windows平台事件循环策略设置
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(speed_test.run_test())
    except KeyboardInterrupt:
        print('\n测试已中止')
    except Exception as e:
        print(f'发生未知错误：{str(e)}')
    finally:
        loop.close()

if __name__ == '__main__':
    main()


if __name__ == '__main__':
    main()