#!/usr/bin/python2
import crawlera

opener = crawlera.CreateUrlOpener()

resp = opener.open('https://www.cee.siemens.com/robots.txt')

content = resp.read()
print content
print resp.url
print resp.code

