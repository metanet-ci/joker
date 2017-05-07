#!/usr/bin/env python

import getopt
import os
import shutil
import sys
import tempfile


def usage():
    sys.stderr.write("%s -p|--program <program-to-visualize> -o|--ofile <output-file-to-write>\n" % sys.argv[0])
    sys.stderr.write("Sample: %s -p \"{[(1),(2,3)]*1,[(4,5),(6)]*3,[(7)]*1}\" -o prog.pdf\n" % sys.argv[0])


class RawLexer:
    spaces = {" ": 0, "\t": 1, "\n": 2}
    numbers = {"0": 0, "1": 1, "2": 2, "3": 3, "4": 4,
               "5": 5, "6": 6, "7": 7, "8": 8, "9": 9}

    def __init__(self, it):
        self.iterator = it
        self.peeked = None

    def next(self):
        if self.peeked != None:
            item = self.peeked
            self.peeked = None
        else:
            try:
                item = self.iterator.next()
            except StopIteration:
                return None
        while item in RawLexer.spaces:
            try:
                item = self.iterator.next()
            except StopIteration:
                return None
        if item not in RawLexer.numbers and not item.isalpha():
            return item

        result = item
        while True:
            try:
                self.peeked = self.iterator.next()
            except StopIteration:
                self.peeked = None
            if self.peeked not in RawLexer.numbers and not self.peeked.isalpha():
                break
            result = result + self.peeked
        return result


class Lexer:
    def __init__(self, it):
        self.rlexer = RawLexer(it)
        self.peeked = None

    def next(self):
        if self.peeked == None:
            return self.rlexer.next()
        else:
            c = self.peeked
            self.peeked = None
            return c

    def peek(self):
        if self.peeked == None:
            self.peeked = self.rlexer.next()
        return self.peeked


class Parse:
    @staticmethod
    def eat(seen, expected):
        if (seen != expected):
            raise Exception('Parsing error, unexpected text. Found: ' + seen + 'was expecting: ' + expected)


class Program:
    def __init__(self):
        self.regions = []

    def addRegion(self, region):
        self.regions.append(region)

    def getRegions(self):
        return self.regions

    @staticmethod
    def parse(lexer):
        program = Program()
        c = lexer.next()
        Parse.eat(c, "{")
        done = False
        first = True
        while not done:
            c = lexer.peek()
            if c == "}":
                c = lexer.next()
                Parse.eat(c, "}")
                done = True
            else:
                if not first:
                    c = lexer.next()
                    Parse.eat(c, ",")
                region = Region.parse(lexer)
                program.addRegion(region)
            first = False
        return program

    def getText(self):
        result = "{"
        regions = self.getRegions()
        if (len(regions) > 0):
            region = regions[0]
            result = result + region.getText()
            for region in regions[1:]:
                result = result + "," + region.getText()
        result = result + "}"
        return result


class Region:
    def __init__(self):
        self.pipelines = []
        self.numReplicas = 0

    def setNumReplicas(self, numReplicas):
        self.numReplicas = numReplicas

    def getNumReplicas(self):
        return self.numReplicas

    def addPipeline(self, pipeline):
        self.pipelines.append(pipeline)

    def getPipelines(self):
        return self.pipelines

    @staticmethod
    def parse(lexer):
        region = Region()
        c = lexer.next()
        Parse.eat(c, "[")
        done = False
        first = True
        while not done:
            c = lexer.peek()
            if c == "]":
                c = lexer.next()
                Parse.eat(c, "]")
                done = True
            else:
                if not first:
                    c = lexer.next()
                    Parse.eat(c, ",")
                pipeline = Pipeline.parse(lexer)
                region.addPipeline(pipeline)
            first = False
        c = lexer.next()
        Parse.eat(c, "*")
        replicas = lexer.next()
        region.setNumReplicas(int(replicas))
        return region

    def getText(self):
        result = "["
        pipelines = self.getPipelines()
        if (len(pipelines) > 0):
            pipeline = pipelines[0]
            result = result + pipeline.getText()
            for pipeline in pipelines[1:]:
                result = result + "," + pipeline.getText()
        result = result + "]"
        result = result + "x" + str(self.getNumReplicas())
        return result


class Pipeline:
    def __init__(self):
        self.operators = []

    def addOperator(self, operator):
        self.operators.append(operator)

    def getOperators(self):
        return self.operators

    @staticmethod
    def parse(lexer):
        pipeline = Pipeline()
        c = lexer.next()
        Parse.eat(c, "(")
        done = False
        first = True
        while not done:
            c = lexer.peek()

            if c == ")":
                c = lexer.next()
                Parse.eat(c, ")")
                done = True
            else:
                if not first:
                    c = lexer.next()
                    Parse.eat(c, ",")
                operator = lexer.next()
                pipeline.addOperator(operator)
            first = False
        return pipeline

    def getText(self):
        result = "("
        operators = self.getOperators()
        if (len(operators) > 0):
            operator = operators[0]
            result = result + operator
            for operator in operators[1:]:
                result = result + "," + operator
        result = result + ")"
        return result


def printProgramLatex(program, file):
    print >> file, "\\documentclass{article}"
    print >> file, "\\usepackage{tikz}"
    print >> file, "\\usetikzlibrary{external}"
    print >> file, "\\usetikzlibrary{fit}"
    print >> file, "\\usetikzlibrary{arrows}"
    print >> file, "\\begin{document}"
    print >> file, "\\begin{center}"
    print >> file, "\\resizebox{\\linewidth}{!}{"
    print >> file, "\\tikzstyle{region} = [rectangle,draw]"
    print >> file, "\\tikzstyle{pipeline} = [rectangle,rounded corners,draw]"
    print >> file, "\\tikzstyle{oper} = [circle,fill=white,node distance=0.2\\linewidth,draw]"
    print >> file, "\\tikzstyle{joint} = [circle,fill=black,draw]"
    print >> file, "\\tikzstyle{stream} = [-latex',draw]"
    print >> file, "\\tikzstyle{line} = [draw]"
    print >> file, "\\begin{tikzpicture}[auto]"
    for (regionIndex, region) in enumerate(program.getRegions()):
        if (regionIndex == 0):
            print >> file, "\\node[joint](j%s){};" % regionIndex
        else:
            print >> file, "\\node[joint,right of=%s](j%s){};" % (lastBox, regionIndex)
        lastBox = "j%s" % regionIndex
        for replica in xrange(0, region.getNumReplicas()):
            for (pipelineIndex, pipeline) in enumerate(region.getPipelines()):
                operList = ""
                for (operatorIndex, operator) in enumerate(pipeline.getOperators()):
                    if replica == 0:
                        print >> file, "\\node[oper,right of=%s](o%s_%s){$o_{%s,%s}$};" % (lastBox, operator, replica, operator, replica)
                        lastBox = "o%s_%s" % (operator, replica)
                    else:
                        print >> file, "\\node[oper,above of=o%s_%s](o%s_%s){$o_{%s,%s}$};" % (
                        operator, replica - 1, operator, replica, operator, replica)
                    operList = operList + ("(o%s_%s)" % (operator, replica))
                print >> file, "\\node[pipeline,fit=%s] (t%s_%s) {};" % (operList, pipelineIndex, replica)
    print >> file, "\\node[joint,right of=%s](j%s){};" % (lastBox, len(program.getRegions()))
    for (regionIndex, region) in enumerate(program.getRegions()):
        for replica in xrange(0, region.getNumReplicas()):
            for (pipelineIndex, pipeline) in enumerate(region.getPipelines()):
                lastOperator = ""
                for (operatorIndex, operator) in enumerate(pipeline.getOperators()):
                    if operatorIndex > 0:
                        print >> file, "\\path[stream] (o%s_%s) -- (o%s_%s);" % (lastOperator, replica, operator, replica)
                    if pipelineIndex == 0 and operatorIndex == 0:
                        print >> file, "\\path[stream] (j%s) -- (o%s_%s);" % (regionIndex, operator, replica)
                    if pipelineIndex == len(region.getPipelines()) - 1 and operatorIndex == len(pipeline.getOperators()) - 1:
                        print >> file, "\\path[line] (o%s_%s) -- (j%s);" % (operator, replica, regionIndex + 1)
                    lastOperator = operator
                if pipelineIndex > 0:
                    lastOp = region.getPipelines()[pipelineIndex - 1].getOperators()[-1]
                    firstOp = pipeline.getOperators()[0]
                    print >> file, "\\path[stream] (o%s_%s) -- (o%s_%s);" % (lastOp, replica, firstOp, replica)
    print >> file, "\\end{tikzpicture}"
    print >> file, "}"
    print >> file, "\\end{center}"
    print >> file, "\\end{document}"
    file.flush()


if __name__ == '__main__':
    try:
        optlist, remaining = getopt.getopt(sys.argv[1:], "hp:o:", ["help", "program=", "ofile="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    programText = ""
    outputFile = ""
    for opt, arg in optlist:
        if opt in ("-h"):
            usage()
            sys.exit(0)
        elif opt in ("-p", "--program"):
            programText = arg
        elif opt in ("-o", "--ofile"):
            outputFile = arg
    lexer = Lexer(iter(programText))
    program = Program.parse(lexer)
    try:
        mydir = os.getcwd()
        tmpdir = tempfile.mkdtemp()
        (tmpfd, tmpfile) = tempfile.mkstemp(dir=tmpdir)
        printProgramLatex(program, os.fdopen(tmpfd, "w"))
        os.chdir(tmpdir)
        os.system("latex '%s' > /dev/null" % tmpfile)
        os.system("dvips '%s'.dvi > /dev/null 2>&1" % tmpfile)
        os.system("ps2pdf '%s'.ps > /dev/null" % tmpfile)
        os.chdir(mydir)
        os.rename("%s.pdf" % tmpfile, outputFile)
    finally:
        shutil.rmtree(tmpdir)
