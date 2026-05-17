import os
import librosa
import librosa.display
import matplotlib
# 使用非互動式後端，這在伺服器環境下至關重要
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from scipy.io import wavfile
from datetime import datetime
from .ai_model import run_inference
import soundfile as sf
import tensorflow as tf
import gc  # 新增：垃圾回收模組

from scipy.signal import butter, sosfiltfilt, decimate, get_window, lfilter, hilbert
from numpy.fft import fft, fftfreq

# --- YAMNet 參數設定 ---

class YAMNetParams:
    sample_rate: float = 16000.0
    stft_window_seconds: float = 0.025
    stft_hop_seconds: float = 0.010
    mel_bands: int = 64
    mel_min_hz: float = 125.0
    mel_max_hz: float = 7500.0
    log_offset: float = 0.001
    patch_window_seconds: float = 0.96
    patch_hop_seconds: float = 0.48
    tflite_compatible: bool = False

def waveform_to_log_mel_spectrogram_patches(waveform, params):
    # 強制在 CPU 上執行，避免多行程式中出現 CUDA_ERROR_INVALID_HANDLE
    with tf.device('/CPU:0'):
        if not tf.is_tensor(waveform):
            waveform = tf.convert_to_tensor(waveform, dtype=tf.float32)

        with tf.name_scope('log_mel_features'):
            window_length_samples = int(round(params.sample_rate * params.stft_window_seconds))
            hop_length_samples = int(round(params.sample_rate * params.stft_hop_seconds))
            fft_length = 2 ** int(np.ceil(np.log(window_length_samples) / np.log(2.0)))
            num_spectrogram_bins = fft_length // 2 + 1
            
            # 計算幅度頻譜圖 (TFLite 相容性分支已移除，因為邏輯相同)
            magnitude_spectrogram = tf.abs(tf.signal.stft(
                signals=waveform,
                frame_length=window_length_samples,
                frame_step=hop_length_samples,
                fft_length=fft_length))

            linear_to_mel_weight_matrix = tf.signal.linear_to_mel_weight_matrix(
                num_mel_bins=params.mel_bands,
                num_spectrogram_bins=num_spectrogram_bins,
                sample_rate=params.sample_rate,
                lower_edge_hertz=params.mel_min_hz,
                upper_edge_hertz=params.mel_max_hz)
                
            mel_spectrogram = tf.matmul(
                magnitude_spectrogram, linear_to_mel_weight_matrix)
            log_mel_spectrogram = tf.math.log(mel_spectrogram + params.log_offset)

            return log_mel_spectrogram

# --- DEMON 參數與輔助函式 ---

CLASSIC_DEMON_PARAMS = {
    'BANDPASS_LOW': 2000, 'BANDPASS_HIGH': 7500, 'DOWNSAMPLE_RATE': 2000,
    'WINDOW_SIZE': 2048, 'WINDOW_OVERLAP_RATIO': 0.95, 'WINDOW_TYPE': 'hann',
    'FREQ_YLIM': 200
}

def _bandpass_filter(signal, fs, lowcut, highcut, order=5):
    nyq = 0.5 * fs
    low = lowcut / nyq
    high = highcut / nyq
    sos = butter(order, [low, high], btype='band', output='sos')
    return sosfiltfilt(sos, signal)

def _square_law_demodulate(signal):
    return signal ** 2

# --- 核心繪圖函式 (已加入記憶體保護) ---

def save_spectrogram(y, sr, out_path_display, out_path_training, spec_type='mel', spec_params=None):
    """
    儲存頻譜圖。
    
    參數:
        y: 音訊資料
        sr: 取樣率
        out_path_display: 顯示用頻譜圖路徑
        out_path_training: 訓練用頻譜圖路徑
        spec_type: 頻譜圖類型 ('mel', 'stft', 'classic_demon', 'envelope_spectrum', 'yamnet_log_mel')
        spec_params: 頻譜圖參數字典，包含:
            - n_fft: FFT window size (預設 1024)
            - hop_length: 步幅 (預設 512)
            - window_type: 窗函數類型 (預設 'hann')
            - n_mels: Mel 濾波器數量 (預設 128)
            - f_min: 最低頻率 (預設 0)
            - f_max: 最高頻率 (預設 sr/2)
            - power: 功率指數 (預設 2.0)
    """
    # 預設參數
    if spec_params is None:
        spec_params = {}
    
    n_fft = spec_params.get('n_fft', 1024)
    hop_length = spec_params.get('hop_length', 512)
    window_type = spec_params.get('window_type', 'hann')
    n_mels = spec_params.get('n_mels', 128)
    f_min = spec_params.get('f_min', 0)
    f_max = spec_params.get('f_max', 0)
    power = spec_params.get('power', 2.0)
    
    # 如果 f_max 為 0，使用 Nyquist 頻率
    if f_max <= 0 or f_max > sr / 2:
        f_max = sr / 2
    
    # 分流處理特殊圖形
    if spec_type == 'classic_demon':
        save_classic_demon_plot(y, sr, out_path_display, out_path_training, spec_params)
        return
    elif spec_type == 'envelope_spectrum':
        save_envelope_spectrum_plot(y, sr, out_path_display, out_path_training, spec_params)
        return
    elif spec_type == 'yamnet_log_mel':
        save_yamnet_log_mel_plot(y, sr, out_path_display, out_path_training, spec_params)
        return

    # 標準 STFT 處理
    fig = None
    try:
        fig, ax = plt.subplots(figsize=(6, 4))
        
        time_str = ""
        if 'time_start' in spec_params and 'time_end' in spec_params:
            time_str = f" ({spec_params['time_start']:.1f}s - {spec_params['time_end']:.1f}s)"
        
        if spec_type == 'stft':
            D = librosa.stft(
                y, 
                n_fft=n_fft, 
                hop_length=hop_length,
                win_length=n_fft,
                window=window_type
            )
            if power == 2.0:
                S_db = librosa.power_to_db(np.abs(D)**2, ref=np.max)
            else:
                S_db = librosa.amplitude_to_db(np.abs(D), ref=np.max)
            display_data = S_db
        else:
            return

        specshow_kwargs = {
            'sr': sr,
            'x_axis': 'time',
            'ax': ax,
            'y_axis': 'hz'
        }

        # 預防 Matplotlib 繪製極度密集的數據矩陣時引發 OOM (Signal 9 SIGKILL)
        # 對時間軸進行 Max Pooling 降採樣
        if display_data.shape[1] > 2000:
            factor = display_data.shape[1] // 1000
            trunc_len = (display_data.shape[1] // factor) * factor
            display_data = display_data[:, :trunc_len].reshape(display_data.shape[0], -1, factor).max(axis=2)
            hop_length = hop_length * factor

        specshow_kwargs['hop_length'] = hop_length

        # 1. 繪製顯示用圖 (包含座標軸與標題)
        librosa.display.specshow(display_data, **specshow_kwargs)
        ax.set_title(f'STFT Spectrogram{time_str}')
        fig.colorbar(ax.collections[0], ax=ax, format='%+2.0f dB')
        plt.tight_layout()
        plt.savefig(out_path_display, dpi=100)
        
        # 2. 清除內容並繪製訓練用圖 (無座標軸純圖)
        fig.clear()
        ax = fig.add_subplot(111)
        librosa.display.specshow(display_data, sr=sr, ax=ax, hop_length=hop_length)
        ax.axis('off')
        fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        plt.savefig(out_path_training, bbox_inches='tight', pad_inches=0, dpi=100)
    
    except Exception as e:
        print(f"繪圖失敗 ({spec_type}): {e}")
    finally:
        # 強制關閉圖表釋放記憶體
        if fig:
            plt.close(fig)
        plt.close('all')

def save_yamnet_log_mel_plot(y, sr, out_path_display, out_path_training, spec_params=None):
    """繪製 YAMNet 格式的 Log Mel 頻譜圖"""
    fig = None
    try:
        params = YAMNetParams()
        if spec_params and 'window_overlap' in spec_params:
            overlap_ratio = spec_params.get('window_overlap', 0.6)  # Default YAMNet overlap is 60% (15ms / 25ms)
            params.stft_hop_seconds = params.stft_window_seconds * (1.0 - overlap_ratio)
        
        # 確保取樣率為 16000 Hz (YAMNet 要求)
        if sr != params.sample_rate:
            y = librosa.resample(y, orig_sr=sr, target_sr=int(params.sample_rate))
        
        log_mel_spectrogram = waveform_to_log_mel_spectrogram_patches(y, params)
        data_to_plot = np.array(log_mel_spectrogram).T

        fig, ax = plt.subplots(figsize=(9.69, 3.7)) 
        hop_length = int(params.sample_rate * params.stft_hop_seconds)
        
        img = librosa.display.specshow(
            data_to_plot, 
            sr=params.sample_rate, 
            hop_length=hop_length, 
            x_axis='time', 
            y_axis='mel', 
            fmin=params.mel_min_hz, 
            fmax=params.mel_max_hz,
            ax=ax,
            cmap='viridis'
        )
        
        time_str = ""
        if spec_params and 'time_start' in spec_params and 'time_end' in spec_params:
            time_str = f" ({spec_params['time_start']:.1f}s - {spec_params['time_end']:.1f}s)"
        ax.set_title(f"Mel Spectrogram{time_str}")
        fig.colorbar(img, ax=ax, format='%+2.0f dB')
        plt.tight_layout()
        plt.savefig(out_path_display, dpi=100)
        
        fig.clear()
        ax = fig.add_subplot(111)
        librosa.display.specshow(
            data_to_plot, 
            sr=params.sample_rate, 
            hop_length=hop_length,
            ax=ax,
            cmap='viridis'
        )
        ax.axis('off')
        fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        plt.savefig(out_path_training, dpi=100, bbox_inches='tight', pad_inches=0)
    except Exception as e:
        print(f"繪製 YAMNet Log Mel 頻譜圖時發生錯誤: {e}")
    finally:
        if fig: plt.close(fig)
        plt.close('all')

def save_classic_demon_plot(segment, sr, out_path_display, out_path_training, spec_params=None):
    fig = None
    try:
        params = CLASSIC_DEMON_PARAMS
        nyquist = sr / 2
        bandpass_high = min(params['BANDPASS_HIGH'], nyquist * 0.99)
        if params['BANDPASS_LOW'] >= bandpass_high: return
        filtered = _bandpass_filter(segment, sr, params['BANDPASS_LOW'], bandpass_high)
        demodulated = _square_law_demodulate(filtered)
        decimation_factor = max(1, int(sr / params['DOWNSAMPLE_RATE']))
        decimated_signal = decimate(demodulated, decimation_factor)
        processed_signal = decimated_signal - np.mean(decimated_signal)
        fs_demo = sr // decimation_factor
        window = get_window(params['WINDOW_TYPE'], params['WINDOW_SIZE'])
        S, freqs, times, _ = plt.specgram(processed_signal, NFFT=params['WINDOW_SIZE'], Fs=fs_demo, window=window, noverlap=int(params['WINDOW_SIZE'] * params['WINDOW_OVERLAP_RATIO']))
        S_db = 10 * np.log10(S + 1e-9)

        fig, ax = plt.subplots(figsize=(6, 4))
        ax.pcolormesh(times, freqs, S_db, cmap='viridis', shading='auto')
        ax.set_ylim(0, params['FREQ_YLIM'])
        ax.set_ylabel('Modulation Frequency (Hz)')
        ax.set_xlabel('Time (s)')
        
        time_str = ""
        if spec_params and 'time_start' in spec_params and 'time_end' in spec_params:
            time_str = f" ({spec_params['time_start']:.1f}s - {spec_params['time_end']:.1f}s)"
        ax.set_title(f"Classic DEMON Spectrogram (2D){time_str}")
        fig.colorbar(ax.collections[0], ax=ax, label='Amplitude (dB)')
        plt.tight_layout()
        plt.savefig(out_path_display, dpi=100)
        
        fig.clear()
        ax = fig.add_subplot(111)
        ax.pcolormesh(times, freqs, S_db, cmap='viridis', shading='auto')
        ax.set_ylim(0, params['FREQ_YLIM'])
        ax.axis('off')
        fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        plt.savefig(out_path_training, bbox_inches='tight', pad_inches=0, dpi=100)
    finally:
        if fig: plt.close(fig)
        plt.close('all')

def save_envelope_spectrum_plot(segment, sr, out_path_display, out_path_training, spec_params=None):
    fig = None
    try:
        nyquist = sr / 2
        bp_low, bp_high = 2000, min(20000, nyquist * 0.99)
        if bp_low >= bp_high: return
        
        b, a = butter(4, [bp_low, bp_high], btype='band', fs=sr)
        segment_filt = lfilter(b, a, segment)
        envelope = np.abs(hilbert(segment_filt))
        
        N = len(envelope)
        if N == 0: return
        
        yf = fft(envelope - np.mean(envelope))
        xf = fftfreq(N, 1 / sr)
        half_N = N // 2
        xf_positive, yf_positive = xf[:half_N], 2.0/N * np.abs(yf[:half_N])
        
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.plot(xf_positive, yf_positive)
        
        time_str = ""
        if spec_params and 'time_start' in spec_params and 'time_end' in spec_params:
            time_str = f" ({spec_params['time_start']:.1f}s - {spec_params['time_end']:.1f}s)"
        ax.set_title(f'Envelope Spectrum (DEMON 1D){time_str}')
        ax.set_xlabel('Modulation Frequency (Hz)')
        ax.set_ylabel('Magnitude')
        ax.grid(True)
        ax.set_xlim(0, 300)
        plt.tight_layout()
        plt.savefig(out_path_display, dpi=100)

        fig.clear()
        ax = fig.add_subplot(111)
        ax.plot(xf_positive, yf_positive)
        ax.set_xlim(0, 300)
        ax.axis('off')
        fig.subplots_adjust(left=0, right=1, bottom=0, top=1)
        plt.savefig(out_path_training, bbox_inches='tight', pad_inches=0, dpi=100)
    except Exception as e:
        print(f"處理包絡線頻譜時發生錯誤: {e}")
    finally:
        if fig: plt.close(fig)
        plt.close('all')

# --- 記憶體優化處理流程 ---

def process_large_audio(filepath, result_dir, spec_type, segment_duration=2.0, overlap_ratio=0.5, target_sr=None, is_mono=True, progress_callback=None, spec_params=None):
    """
    以逐段精確載入的方式處理大型音訊檔案，確保所有片段長度一致且節省記憶體。
    
    參數:
        spec_params: 頻譜圖參數字典，傳遞給 save_spectrogram 函式
    """
    all_results = []
    basename = f"{os.path.splitext(os.path.basename(filepath))[0]}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    try:
        # 新增容錯讀取機制
        try:
            info = sf.info(filepath)
            original_sr = info.samplerate
            total_samples = info.frames
        except Exception as e:
            print(f"SoundFile 無法讀取 {filepath}，嘗試使用 Librosa Fallback。錯誤: {e}")
            original_sr = librosa.get_samplerate(filepath)
            total_duration = librosa.get_duration(path=filepath)
            total_samples = int(total_duration * original_sr)

        # YAMNet 強制 16000 Hz
        if spec_type == 'yamnet_log_mel':
            sr = 16000
        else:
            sr = target_sr if target_sr else original_sr

        frame_length = int(segment_duration * sr)

        if total_samples < frame_length:
            print("警告：音訊檔案總長度小於設定的單一片段長度。")
            y_segment, _ = librosa.load(filepath, sr=sr, mono=is_mono)
            num_samples = y_segment.shape[-1]
            if num_samples < frame_length:
                pad_width = [(0, 0)] * y_segment.ndim
                pad_width[-1] = (0, frame_length - num_samples)
                y_segment = np.pad(y_segment, pad_width)
            segments_to_process = [(0, y_segment)]
        else:
            total_duration_sec = total_samples / original_sr
            step_sec = segment_duration * (1 - overlap_ratio)
            start_seconds = np.arange(0, total_duration_sec - segment_duration + 0.001, step_sec)
            segments_to_process = [(s, None) for s in start_seconds]

        total_segments = len(segments_to_process)
        
        # 迴圈處理
        for i, (start_s, preloaded_segment) in enumerate(segments_to_process):
            # 1. 處理音訊資料
            if preloaded_segment is not None:
                y_segment = preloaded_segment
            else:
                y_segment, _ = librosa.load(
                    filepath, 
                    sr=sr, 
                    mono=is_mono, 
                    offset=start_s, 
                    duration=segment_duration
                )

            num_samples = y_segment.shape[-1]
            if num_samples < frame_length:
                pad_width = [(0, 0)] * y_segment.ndim
                pad_width[-1] = (0, frame_length - num_samples)
                y_segment = np.pad(y_segment, pad_width)
            
            y_segment = y_segment[..., :frame_length]

            # 2. 設定檔名路徑
            audio_filename = f"{basename}_part{i}.wav"
            display_spec_filename = f"{basename}_spec_display_{i}.png"
            training_spec_filename = f"{basename}_spec_training_{i}.png"

            audio_path = os.path.join(result_dir, audio_filename)
            display_spec_path = os.path.join(result_dir, display_spec_filename)
            training_spec_path = os.path.join(result_dir, training_spec_filename)
            
            # 3. 儲存切割音檔 (確保正確處理多聲道)
            if y_segment.ndim > 1:
                # 預防立體聲反相抵消 (Phase Cancellation) 導致輸出全黑/無聲
                y_mono = librosa.to_mono(y_segment)
                if np.max(np.abs(y_mono)) < 1e-4 and np.max(np.abs(y_segment)) > 1e-3:
                    y_segment = y_segment[0] # 若平均後消失，代表反相，直接取用左聲道
                else:
                    y_segment = y_mono
                    
            audio_int16 = (y_segment * 32767).astype(np.int16)
            wavfile.write(audio_path, sr, audio_int16)
            
            # 4. 繪製頻譜圖 (這裡最耗記憶體)
            mono_segment = y_segment
            if spec_params is None:
                spec_params = {}
            spec_params['time_start'] = start_s
            spec_params['time_end'] = start_s + segment_duration
            
            save_spectrogram(mono_segment, sr, display_spec_path, training_spec_path, spec_type, spec_params)
            
            # 5. 加入結果列表
            all_results.append({
                'audio': audio_filename,
                'display_spectrogram': display_spec_filename,
                'training_spectrogram': training_spec_filename,
                'detections': run_inference(training_spec_path)
            })
            
            if progress_callback:
                progress_callback(i + 1, total_segments)
            
            # 每處理 10 張圖，就強制執行一次垃圾回收
            if i % 10 == 0:
                plt.close('all') 
                gc.collect()    
        
    except Exception as e:
        print(f"處理大型音訊檔案時發生錯誤: {e}")
        raise e
        
    return all_results