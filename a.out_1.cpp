/* Template for generating a child module for query */
#if defined(__clang__) || (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#endif
#include "eclinclude4.hpp"
#define OPTIMIZE __attribute__((optimize(3)))
#include "eclrtl.hpp"
#include "eclhelper.hpp"

namespace KafkaPlugin { extern int getTopicPartitionCount(ICodeContext * ctx,const char * brokers,const char * topic); }
extern void user1(ICodeContext * ctx,char * __result);
extern "C"  bool fsSuperFileExists(ICodeContext * ctx,const char * lsuperfn);
extern "C"  void fsCreateSuperFile(ICodeContext * ctx,const char * lsuperfn,bool sequentialparts,bool ifdoesnotexist);
namespace KafkaPlugin { extern IRowStream * getMessageDataset(ICodeContext * ctx,IEngineRowAllocator * _resultAllocator,const char * brokers,const char * topic,const char * consumergroup,int partitionnum,long long maxrecords); }
extern RTL_API unsigned rtlTrimStrLen(size32_t lenSrc,const char * src);
extern "C"  void fsStartSuperFileTransaction(IGlobalCodeContext * gctx);
extern "C"  void fsAddSuperFile(IGlobalCodeContext * gctx,const char * lsuperfn,const char * lfn,unsigned atpos,bool addcontents,bool strict);
extern "C"  void fsFinishSuperFileTransaction(IGlobalCodeContext * gctx,bool rollback);


#include "a.out.hpp"

struct cAc2 : public CThorInlineTableArg {
	virtual IOutputMetaData * queryOutputMeta() { return &qmx1(); }
	virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) {
		ctx = _ctx;
		allocP.setown(ctx->getRowAllocator(&qmx2(),2U));
		vU = ctx->getResultInt("gl6",4294967293U);
		v11 = (int)ctx->getResultInt("wf2",4294967293U);
	}
	Owned<IEngineRowAllocator> allocP;
	long long vU;
	int v11;
	virtual size32_t getRow(ARowBuilder & crSelf, __uint64 row) {
		crSelf.getSelf();
		Owned<IRowStream> vT;
		vT.setown(KafkaPlugin::getMessageDataset(ctx,allocP,"192.168.43.34","TestTopic","hpcc",(int)((long long)(row+1) - 1LL),vU));
		RtlLinkedDatasetBuilder crV(allocP);
		rtlRowAttr r01;
		for (;;) {
			r01.setown(vT.get()->nextRow());
			if (!r01.getbytes()) break;
			crV.append(r01.getbytes());
		}
		*((unsigned *)(crSelf.row() + 0U)) = crV.getcount();
		*((byte * * *)(crSelf.row() + 4U)) = crV.linkrows();
		return 12U;
	}
	virtual unsigned long long numRows() {
		return (unsigned long long)v11;
	}
	virtual unsigned getFlags() { return TTFdistributed|TTFnoconstant; }
};
extern "C" ECL_API IHThorArg * fAc2() { return new cAc2; }
struct cAc3 : public CThorNormalizeChildArg {
	struct M81Class : public CNormalizeLinkedChildIterator {
		virtual void init(const void * _left) {
			const byte * left = (const byte *)_left;
			setDataset(*((unsigned *)(left + 0U)),*((byte * * *)(left + 4U)));
		}
	} m81;
	virtual IOutputMetaData * queryChildRecordSize() { return &qmx2(); }
	virtual IOutputMetaData * queryOutputMeta() { return &qmx4(); }
	INormalizeChildIterator * queryIterator() { return &m81; }
	virtual size32_t transform(ARowBuilder & crSelf, const void * _left, const void * _right, unsigned counter) {
		crSelf.getSelf();
		const unsigned char * left = (const unsigned char *) _left;
		const unsigned char * right = (const unsigned char *) _right;
		*((unsigned char *)(crSelf.row() + 0U)) = 0U;
		return 1U;
	}
};
extern "C" ECL_API IHThorArg * fAc3() { return new cAc3; }
// use library for cAc4
extern "C" ECL_API IHThorArg * fAc4() {
	return new CLibraryExistsAggregateArg(&qmx5());
}
struct cAc5 : public CThorRemoteResultArg {
	virtual IOutputMetaData * queryOutputMeta() { return &qmx5(); }
	virtual void sendResult(const void * _self) {
		const unsigned char * self = (const unsigned char *)_self;
		bool vA1;
		vA1 = *((bool *)(self + 0U));
		ctx->setResultBool("aD",4294967293U,vA1);
	}
};
extern "C" ECL_API IHThorArg * fAc5() { return new cAc5; }
struct cAc7 : public CThorInlineTableArg {
	virtual IOutputMetaData * queryOutputMeta() { return &qmx1(); }
	virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) {
		ctx = _ctx;
		allocF1.setown(ctx->getRowAllocator(&qmx2(),7U));
		vH1 = ctx->getResultInt("gl8",4294967293U);
		vK1 = (int)ctx->getResultInt("wf2",4294967293U);
	}
	Owned<IEngineRowAllocator> allocF1;
	long long vH1;
	int vK1;
	virtual size32_t getRow(ARowBuilder & crSelf, __uint64 row) {
		crSelf.getSelf();
		Owned<IRowStream> vG1;
		vG1.setown(KafkaPlugin::getMessageDataset(ctx,allocF1,"192.168.43.34","TestTopic","hpcc",(int)((long long)(row+1) - 1LL),vH1));
		RtlLinkedDatasetBuilder crI1(allocF1);
		rtlRowAttr rJ1;
		for (;;) {
			rJ1.setown(vG1.get()->nextRow());
			if (!rJ1.getbytes()) break;
			crI1.append(rJ1.getbytes());
		}
		*((unsigned *)(crSelf.row() + 0U)) = crI1.getcount();
		*((byte * * *)(crSelf.row() + 4U)) = crI1.linkrows();
		return 12U;
	}
	virtual unsigned long long numRows() {
		return (unsigned long long)vK1;
	}
	virtual unsigned getFlags() { return TTFdistributed|TTFnoconstant; }
};
extern "C" ECL_API IHThorArg * fAc7() { return new cAc7; }
struct cAc8 : public CThorNormalizeLinkedChildArg {
	virtual IOutputMetaData * queryOutputMeta() { return &qmx2(); }
	virtual byte * first(const void * parentRecord) {
		const byte * left = (const byte *)parentRecord;
		iterL1.init(*((unsigned *)(left + 0U)), *((byte * * *)(left + 4U)));
		return (byte *)iterL1.first();
	}
	RtlSafeLinkedDatasetCursor iterL1;
	virtual byte * next() {
		return (byte *)iterL1.next();
	}
};
extern "C" ECL_API IHThorArg * fAc8() { return new cAc8; }
struct cAc9 : public CThorCsvWriteArg {
	struct CsvClass : public ICsvParameters {
		virtual bool queryEBCDIC() { return false; }
		virtual const char * getHeader() { return NULL; }
		virtual unsigned queryHeaderLen() { return 0; }
		virtual size32_t queryMaxSize() { return 4096; }
		virtual const char * getQuote(unsigned idx) {
			return NULL;
		}
		virtual const char * getSeparator(unsigned idx) {
			switch (idx) {
				case 0:
					return ",";
				default: {
					return NULL;
				}
			}
		}
		virtual const char * getTerminator(unsigned idx) {
			switch (idx) {
				case 0:
					return "\n";
				default: {
					return NULL;
				}
			}
		}
		virtual const char * getEscape(unsigned idx) {
			return NULL;
		}
		virtual unsigned getFlags() { return defaultQuote|defaultEscape; }
	} csv;
	virtual ICsvParameters * queryCsvParameters() { return &csv; }
	virtual unsigned getFlags() { return TDXvarfilename|TDWupdatecrc; }
	virtual int getSequence() { return 0; }
	virtual unsigned getFormatCrc() {
		return 673165029U;
	}
	virtual IOutputMetaData * queryDiskRecordSize() { return &qmx2(); }
	virtual IOutputMetaData * queryOutputMeta() { return &qmx2(); }
	virtual void onCreate(ICodeContext * _ctx, IHThorArg *, MemoryBuffer * in) {
		ctx = _ctx;
		ctx->getResultString(vN1,vM1.refstr(),"glA",4294967293U);
	}
	rtlDataAttr vM1;
	unsigned vN1;
	virtual const char * queryRecordECL() { return ecl2(ctx); }
	virtual void getUpdateCRCs(unsigned & eclCrc, unsigned __int64 & totalCRC) {
		unsigned long long vO1 = 0U;
		vO1 = vO1 ^ (unsigned long long)ctx->getResultHash("wf2",4294967293U);
		eclCrc = 796279617U;
		totalCRC = vO1;
	}
	virtual const char * getFileName() {
		return ctx->cloneVString(vN1,vM1.getstr());
	}
	void writeRow(const byte * self, ITypedOutputStream * out) {
		out->writeUnsigned(*((unsigned *)(self + 0U)));
		out->writeSigned(*((long long *)(self + 4U)));
		out->writeString(rtlTrimStrLen(*((unsigned *)(self + 12U)),(char *)(self + 16U)),(char *)(self + 16U));
	}
};
extern "C" ECL_API IHThorArg * fAc9() { return new cAc9; }

#if defined(__clang__) || (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#pragma GCC diagnostic error "-Wall"
#pragma GCC diagnostic error "-Wextra"
#pragma GCC diagnostic ignored "-Wunused-parameter"
#endif

extern void user1(ICodeContext * ctx,char * __result) {
#line 45 "latestconsume.ecl"
	    
        // Declarations
        struct tm localt; // localtime in "tm" structure
        time_t timeinsecs;  // variable to store time in secs
        //char ret[17];

        // Get time in sec since Epoch
        time(&timeinsecs);
        // Convert to local time
        localtime_r(&timeinsecs,&localt);
        // Format the local time value
        strftime(__result, 18, "%F%H%M%S%u", &localt); // Formats the localtime to YYYY-MM-DDHHMMSSW where W is the weekday

    
#line 221 "a.out_1.cpp"
}

#if defined(__clang__) || (__GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 2))
#pragma GCC diagnostic ignored "-Wall"
#pragma GCC diagnostic ignored "-Wextra"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wparentheses"
#endif


