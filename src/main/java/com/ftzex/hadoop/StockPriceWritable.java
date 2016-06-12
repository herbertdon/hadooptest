package com.ftzex.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.ftzex.utils.CSVParser;
import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class StockPriceWritable implements WritableComparable<StockPriceWritable>, Cloneable {
	String symbol;
	String date;
	double open;
	double high;
	double low;
	double close;
	int volume;

	public StockPriceWritable() {
	}

	public StockPriceWritable(String symbol,
							  String date,
							  double open,
							  double high,
							  double low,
							  double close,
							  int volume,
							  double adjClose) {
		this.symbol = symbol;
		this.date = date;
		this.open = open;
		this.high = high;
		this.low = low;
		this.close = close;
		this.volume = volume;
		this.adjClose = adjClose;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public double getOpen() {
		return open;
	}

	public void setOpen(double open) {
		this.open = open;
	}

	public double getHigh() {
		return high;
	}

	public void setHigh(double high) {
		this.high = high;
	}

	public double getLow() {
		return low;
	}

	public void setLow(double low) {
		this.low = low;
	}

	public double getClose() {
		return close;
	}

	public void setClose(double close) {
		this.close = close;
	}

	public int getVolume() {
		return volume;
	}

	public void setVolume(int volume) {
		this.volume = volume;
	}

	public double getAdjClose() {
		return adjClose;
	}

	public void setAdjClose(double adjClose) {
		this.adjClose = adjClose;
	}

	double adjClose;

	@Override
	public void readFields(DataInput in) throws IOException {
		symbol = WritableUtils.readString(in);
		date = WritableUtils.readString(in);
		open = in.readDouble();
		high = in.readDouble();
		low = in.readDouble();
		close = in.readDouble();
		volume = in.readInt();
		adjClose = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		/**
		 * DataOutput是java.io的接口,可以将一些特定类型的数据转换为二进制流
		 * 实现DataOutput接口的类有DataOutputStream和ObjectOutputStream和RandomAccessFile
		 * WritableUtils对于DataOutput进行了些包装,还可以写一些压缩数据流.
		 * 这里的writeString和java.io.DataOutput的writeChars其实我看来是没什么大的区别的
		 * 只是WritableUtils先将String通过UTF-8编码解码为bytes,然后再写入.感觉是强制UTF-8编码的字符.
		 * 其他编码的字符应该会出错.
		 */
		WritableUtils.writeString(out, symbol);
		WritableUtils.writeString(out, date);
		out.writeDouble(open);
		out.writeDouble(high);
		out.writeDouble(low);
		out.writeDouble(close);
		out.writeInt(volume);
		out.writeDouble(adjClose);
	}

	@Override
	public int compareTo(StockPriceWritable passwd) {
        /**
         * CompareToBuilder是org.apache.commons.lang的工具类,用于辅助实现Comparable.compareTo(object)方法
		 * reflectionCompare是用了反射机制来实现比较,并且支持链式比较,反射获取class,field,以此来进行比较.
		 * 该包下面还有EqualsBuilder来辅助实现Object.equals方法,HashCodeBuilder来实现Object.hashCode方法
		 * ToStringBuilder来实现toString()
		 * 上述builder都有类似append的方法,来加入需要操作的field,比如ToStringBuilder加入field就可以对对象toString
		 * 时候做到格式化输出,当然如果是直接用relection,则是反射field后把反射获得的field都append进来
         */
		return CompareToBuilder.reflectionCompare(this,passwd);
	}

	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
	}

	public static StockPriceWritable fromLine(String line)
			throws IOException {
		CSVParser parser = new CSVParser();
		String[] parts = parser.parseLine(line);

		StockPriceWritable stock = new StockPriceWritable(
				parts[0], parts[1], Double.valueOf(parts[2]),
				Double.valueOf(parts[3]),
				Double.valueOf(parts[4]),
				Double.valueOf(parts[5]),
				Integer.valueOf(parts[6]),
				Double.valueOf(parts[7])
		);
		return stock;
	}


}