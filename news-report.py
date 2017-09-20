#!/usr/bin/env python3
#
# log analysis for the news paper
import psycopg2
import sys


def mostPopularArticles(f):
    #
    # Build SQL to answer the question:
    #    What are the most popular three articles of all time?
    #

    sql = "select title, count(*) as visits " \
          "from articles inner join log " \
          "on slug = substring(path from '/%%/#\"%#\"' for '#') " \
          "group by title " \
          "order by visits desc " \
          "limit 3;"

    articles = accessDB(sql)

    writeOutput(f, "Most popular articles based on view count")
    for title, visits in articles:
        rec = "{0} -- {1}".format(title, visits)
        writeOutput(f, rec)

    writeOutput(f, "")
    writeOutput(f, "")


def mostPopularAuthors(f):
    #
    # Build SQL to answer the question:
    #   Who are the most popular article authors of all time?
    #
    sql = "select (select name from authors where id = a.id) as name, " \
          "       count(*) as visits " \
          "from articles inner join log " \
          "on slug = substring(path from '/%%/#\"%#\"' for '#') " \
          "inner join authors a " \
          "on a.id = articles.author " \
          "group by a.id " \
          "order by visits desc;"

    authors = accessDB(sql)

    writeOutput(f, "Most popular authors based on view count of articles")
    for name, visits in authors:
        rec = "{0} -- {1}".format(name, visits)
        writeOutput(f, rec)

    writeOutput(f, "")
    writeOutput(f, "")


def worstDays(f):
    #
    # Build SQL to answer the question:
    #   On which days did more than 1% of requests lead to errors?
    #
    sql = "select total.date, " \
          "       (error.count::decimal / total.count) * 100. as error_rate " \
          "from (select date(time) as date, count(*) as count" \
          "        from log " \
          "        group by date(time) " \
          "        order by date(time)) total " \
          " inner join " \
          "     (select date(time) as date, count(*) as count " \
          "        from log " \
          "       where status not like '%200%' " \
          "       group by date(time) " \
          "       order by date(time)) error " \
          "on total.date = error.date " \
          "where (error.count::decimal / total.count) > .01 " \
          "order by total.date;" \

    errors = accessDB(sql)

    writeOutput(f, "Dates with errors exceeding 1% threshold")
    for date, error_rate in errors:
        rec = "{0} -- {1:4.2f}%".format(date, error_rate)
        writeOutput(f, rec)


def accessDB(sql):
    db = psycopg2.connect("dbname=news")
    c = db.cursor()

    c.execute(sql)
    results = c.fetchall()
    db.close()

    return results


def writeOutput(f, rec):
    if (f != ''):
        f.write(rec + '\n')
    else:
        print(rec)

if __name__ == '__main__':
    if len(sys.argv) > 1:
        filename = sys.argv[1]
        f = open(filename, 'w')
        print("Output written to " + filename)
    else:
        f = ''

    mostPopularArticles(f)
    mostPopularAuthors(f)
    worstDays(f)

    if f != '':
        f.close
