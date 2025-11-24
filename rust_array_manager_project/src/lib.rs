use pyo3::prelude::*;
use pyo3::types::{PyDateTime, PyDict, PyList, PyTuple};
use numpy::PyArray1;
use ndarray::Array1;
use chrono::{DateTime, Datelike, Timelike};

#[pyclass(module = "rust_array_manager")]
struct ArrayManager {
    count: usize,
    buffer_size: usize,
    additional_status: bool,
    inited: bool,
    datetime_array: Array1<i64>,
    open_array: Array1<f64>,
    high_array: Array1<f64>,
    low_array: Array1<f64>,
    close_array: Array1<f64>,
    return_array: Array1<f64>,
    volume_array: Array1<f64>,
    open_interest_array: Array1<f64>,
    volatility_array: Array1<f64>,
    amplitude_array: Array1<f64>,
}

#[pymethods]
impl ArrayManager {
    #[new]
    #[pyo3(signature = (buffer_size, additional_status=false))]
    fn new(
        buffer_size: usize,
        additional_status: bool,
    ) -> Self {
        ArrayManager {
            count: 0,
            buffer_size,
            additional_status,
            inited: false,
            datetime_array: Array1::zeros(buffer_size),
            open_array: Array1::zeros(buffer_size),
            high_array: Array1::zeros(buffer_size),
            low_array: Array1::zeros(buffer_size),
            close_array: Array1::zeros(buffer_size),
            return_array: Array1::zeros(buffer_size),
            volume_array: Array1::zeros(buffer_size),
            open_interest_array: Array1::zeros(buffer_size),
            volatility_array: Array1::zeros(buffer_size),
            amplitude_array: Array1::zeros(buffer_size),
        }
    }

    /// 实现 __reduce__ 方法用于 pickle 序列化
    fn __reduce__<'py>(&self, py: Python<'py>) -> PyResult<(Py<PyAny>, Bound<'py, PyTuple>, Bound<'py, PyDict>)> {
        // 获取类的构造函数
        let cls = py.get_type::<Self>();
        
        // 构造函数参数 - 直接使用元组
        let args = (
            self.buffer_size,
            self.additional_status,
        );
        let args_tuple = PyTuple::new(py, args.into_pyobject(py)?)?;
        
        // 保存状态字典
        let state = PyDict::new(py);
        state.set_item("count", self.count)?;
        state.set_item("inited", self.inited)?;
        state.set_item("datetime_array", self.datetime_array.to_vec())?;
        state.set_item("open_array", self.open_array.to_vec())?;
        state.set_item("high_array", self.high_array.to_vec())?;
        state.set_item("low_array", self.low_array.to_vec())?;
        state.set_item("close_array", self.close_array.to_vec())?;
        state.set_item("return_array", self.return_array.to_vec())?;
        state.set_item("volume_array", self.volume_array.to_vec())?;
        state.set_item("open_interest_array", self.open_interest_array.to_vec())?;
        state.set_item("volatility_array", self.volatility_array.to_vec())?;
        state.set_item("amplitude_array", self.amplitude_array.to_vec())?;
        
        Ok((cls.into(), args_tuple, state))
    }

    /// 实现 __setstate__ 方法用于 pickle 反序列化
    fn __setstate__(&mut self, state: &Bound<'_, PyDict>) -> PyResult<()> {
        // 恢复基础状态
        self.count = state.get_item("count")?.unwrap().extract()?;
        self.inited = state.get_item("inited")?.unwrap().extract()?;
        
        // 恢复数组数据
        let datetime_vec: Vec<i64> = state.get_item("datetime_array")?.unwrap().extract()?;
        self.datetime_array = Array1::from_vec(datetime_vec);
        
        let open_vec: Vec<f64> = state.get_item("open_array")?.unwrap().extract()?;
        self.open_array = Array1::from_vec(open_vec);
        
        let high_vec: Vec<f64> = state.get_item("high_array")?.unwrap().extract()?;
        self.high_array = Array1::from_vec(high_vec);
        
        let low_vec: Vec<f64> = state.get_item("low_array")?.unwrap().extract()?;
        self.low_array = Array1::from_vec(low_vec);
        
        let close_vec: Vec<f64> = state.get_item("close_array")?.unwrap().extract()?;
        self.close_array = Array1::from_vec(close_vec);
        
        let return_vec: Vec<f64> = state.get_item("return_array")?.unwrap().extract()?;
        self.return_array = Array1::from_vec(return_vec);
        
        let volume_vec: Vec<f64> = state.get_item("volume_array")?.unwrap().extract()?;
        self.volume_array = Array1::from_vec(volume_vec);
        
        let open_interest_vec: Vec<f64> = state.get_item("open_interest_array")?.unwrap().extract()?;
        self.open_interest_array = Array1::from_vec(open_interest_vec);
        
        let volatility_vec: Vec<f64> = state.get_item("volatility_array")?.unwrap().extract()?;
        self.volatility_array = Array1::from_vec(volatility_vec);
        
        let amplitude_vec: Vec<f64> = state.get_item("amplitude_array")?.unwrap().extract()?;
        self.amplitude_array = Array1::from_vec(amplitude_vec);
        
        Ok(())
    }

    fn update_bar(&mut self, bar: &Bound<'_, PyAny>) -> PyResult<()> {
        self.count += 1;
        if !self.inited && self.count >= self.buffer_size {
            self.inited = true;
        }

        let open_price: f64 = bar.getattr("open_price")?.extract()?;
        let high_price: f64 = bar.getattr("high_price")?.extract()?;
        let low_price: f64 = bar.getattr("low_price")?.extract()?;
        let close_price: f64 = bar.getattr("close_price")?.extract()?;
        let volume: f64 = bar.getattr("volume")?.extract()?;

        self.shift_arrays();

        let idx = self.buffer_size - 1;
        self.open_array[idx] = open_price;
        self.high_array[idx] = high_price;
        self.low_array[idx] = low_price;
        self.close_array[idx] = close_price;
        self.volume_array[idx] = volume;

        if open_price != 0.0 {
            self.return_array[idx] = close_price / open_price - 1.0;
        }

        if self.additional_status {
            let datetime = bar.getattr("datetime")?;
            let timestamp = Self::extract_timestamp(&datetime)?;
            self.datetime_array[idx] = timestamp;

            let open_interest: f64 = bar.getattr("open_interest")?.extract()?;
            self.open_interest_array[idx] = open_interest;

            let volatility = self.return_array.std(0.0) * (365f64.sqrt());
            self.volatility_array[idx] = volatility;

            if self.buffer_size >= 2 && self.close_array[idx - 1] != 0.0 {
                let amplitude = (high_price - low_price).abs() / self.close_array[idx - 1];
                self.amplitude_array[idx] = amplitude;
            }
        }

        Ok(())
    }

    #[getter]
    fn datetimes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for &timestamp in self.datetime_array.iter() {
            let dt = DateTime::from_timestamp(timestamp, 0)
                .ok_or_else(|| PyErr::new::<pyo3::exceptions::PyValueError, _>("Invalid timestamp"))?;
            
            let py_dt = PyDateTime::new(
                py,
                dt.year(),
                dt.month() as u8,
                dt.day() as u8,
                dt.hour() as u8,
                dt.minute() as u8,
                dt.second() as u8,
                0,
                None,
            )?;
            list.append(py_dt)?;
        }
        Ok(list)
    }

    #[getter]
    fn open<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.open_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn high<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.high_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn low<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.low_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn close<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.close_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn volume<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.volume_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn returns<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.return_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn volatility<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.volatility_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn amplitude<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.amplitude_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn wt<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let wt_array = &self.return_array / &self.volatility_array;
        PyArray1::from_vec(py, wt_array.to_vec())
    }

    #[getter]
    fn open_interest<'py>(&self, py: Python<'py>) -> Bound<'py, PyArray1<f64>> {
        let slice = self.open_interest_array.as_slice().expect("Array must be contiguous");
        PyArray1::from_slice(py, slice)
    }

    #[getter]
    fn count(&self) -> usize {
        self.count
    }

    #[getter]
    fn inited(&self) -> bool {
        self.inited
    }
}

impl ArrayManager {
    fn shift_arrays(&mut self) {
        let n = self.buffer_size;
        if n > 1 {
            if let Some(s) = self.open_array.as_slice_mut() { s.copy_within(1.., 0); }
            if let Some(s) = self.high_array.as_slice_mut() { s.copy_within(1.., 0); }
            if let Some(s) = self.low_array.as_slice_mut() { s.copy_within(1.., 0); }
            if let Some(s) = self.close_array.as_slice_mut() { s.copy_within(1.., 0); }
            if let Some(s) = self.volume_array.as_slice_mut() { s.copy_within(1.., 0); }
            if let Some(s) = self.return_array.as_slice_mut() { s.copy_within(1.., 0); }
            if self.additional_status {
                if let Some(s) = self.datetime_array.as_slice_mut() { s.copy_within(1.., 0); }
                if let Some(s) = self.open_interest_array.as_slice_mut() { s.copy_within(1.., 0); }
                if let Some(s) = self.volatility_array.as_slice_mut() { s.copy_within(1.., 0); }
                if let Some(s) = self.amplitude_array.as_slice_mut() { s.copy_within(1.., 0); }
            }
        }
    }

    fn extract_timestamp(datetime: &Bound<'_, PyAny>) -> PyResult<i64> {
        let timestamp_method = datetime.call_method0("timestamp")?;
        let timestamp: f64 = timestamp_method.extract()?;
        Ok(timestamp as i64)
    }
}

#[pymodule]
fn rust_array_manager(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ArrayManager>()?;
    Ok(())
}